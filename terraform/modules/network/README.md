# Network

Provisions the VPC and three subnet tiers, one of each per availability zone.

## 1. The tiers

| Tier | Holds |
|---|---|
| `public` | NAT gateways, internet-facing load balancers |
| `private` | EKS worker nodes |
| `database` | RDS and ElastiCache |

The data stores get a tier of their own rather than sharing the node tier. A route table or network ACL change aimed at workers then cannot widen reachability to Postgres and Valkey as a side effect.

## 2. Subnet addressing

You pass a VPC CIDR and a list of availability zones. The module derives every subnet range with `cidrsubnet(cidr, 4, n)`, so adding a zone does not mean hand-computing three more ranges.

With a `/16` VPC each subnet is a `/20`, or 4094 usable addresses. Size the private tier for pods, not nodes: the VPC CNI assigns every pod an IP from the node's subnet, so pod density is what exhausts it.

A validation rejects a CIDR smaller than `/16`, because the three tiers will not fit.

## 3. NAT

`single_nat_gateway = true` routes all private egress through one gateway in one zone. That is one hourly charge instead of three, and it makes outbound traffic depend on that zone staying up. For a crawler, losing egress stops the fetch stage entirely.

Set it to `false` for production and you get one gateway per zone.

## 4. Load balancer discovery

The public and private subnets carry the `kubernetes.io/role/elb` and `kubernetes.io/role/internal-elb` tags. The AWS Load Balancer Controller reads those to decide which subnets to place an ALB or NLB in; without them it fails to provision one and reports no matching subnets.

## 5. Edgecases

### "`tofu plan` fails on the CIDR validation"

Your VPC CIDR is smaller than `/16`. Three `/20` tiers across three zones need the space.

### "Pods stop scheduling with no IPs available"

The private tier ran out of addresses. Every pod holds one, not every node. Either widen the VPC CIDR (which replaces subnets) or reduce pods per node.

### "The Load Balancer Controller cannot find subnets"

It is looking for the `kubernetes.io/role/*` tags. This module sets them; check nothing has stripped them from the live subnets.

## 6. Limits and numbers

| Thing | Value |
|---|---|
| Minimum VPC CIDR | /16 |
| Minimum availability zones | 2 |
| Subnets per zone | 3 |
| Subnet size in a /16 VPC | /20, 4094 usable addresses |
| NAT gateways | 1 by default, one per zone when `single_nat_gateway = false` |
