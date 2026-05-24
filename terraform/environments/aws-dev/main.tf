locals {
  name = var.cluster_name
  tags = {
    Project     = "crawlrs"
    Environment = var.environment
  }

  private_subnets = [for i, az in var.azs : cidrsubnet(var.vpc_cidr, 4, i)]
  public_subnets  = [for i, az in var.azs : cidrsubnet(var.vpc_cidr, 4, i + length(var.azs))]
}

# ---------------------------------------------------------------------------
# Networking
# ---------------------------------------------------------------------------

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = local.name
  cidr = var.vpc_cidr
  azs  = var.azs

  private_subnets  = local.private_subnets
  public_subnets   = local.public_subnets
  database_subnets = [for i, az in var.azs : cidrsubnet(var.vpc_cidr, 4, i + length(var.azs) * 2)]

  create_database_subnet_group = true

  enable_nat_gateway   = true
  single_nat_gateway   = true # dev; prod should use one per AZ
  enable_dns_hostnames = true
  enable_dns_support   = true

  # Tags required by the AWS Load Balancer Controller for auto-
  # discovery of subnets when provisioning ALBs / NLBs.
  public_subnet_tags = {
    "kubernetes.io/role/elb" = 1
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = 1
  }

  tags = local.tags
}

# ---------------------------------------------------------------------------
# EKS
# ---------------------------------------------------------------------------

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = local.name
  cluster_version = var.kubernetes_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  # Public endpoint for kubectl access; restrict in prod via
  # cluster_endpoint_public_access_cidrs.
  cluster_endpoint_public_access = true

  eks_managed_node_groups = {
    crawlrs = {
      instance_types = var.eks_node_instance_types
      min_size       = var.eks_node_min
      max_size       = var.eks_node_max
      desired_size   = var.eks_node_desired

      # EBS-backed pods need the CSI driver; EKS 1.30+ includes it
      # as a managed addon but the node IAM role needs the policy.
      iam_role_additional_policies = {
        ebs_csi = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
      }
    }
  }

  # Install the EBS CSI driver as a managed addon so PVCs work
  # out of the box for StatefulSet volumes (Valkey RDB, Postgres
  # data, crawlrs blob storage if using local-PVC mode).
  cluster_addons = {
    aws-ebs-csi-driver = {
      most_recent = true
    }
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      most_recent = true
    }
  }

  tags = local.tags
}

# ---------------------------------------------------------------------------
# Valkey (ElastiCache Serverless or replication group)
# ---------------------------------------------------------------------------

resource "aws_elasticache_subnet_group" "this" {
  name       = "${local.name}-valkey"
  subnet_ids = module.vpc.private_subnets
  tags       = local.tags
}

resource "aws_security_group" "valkey" {
  name_prefix = "${local.name}-valkey-"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description     = "Valkey from EKS nodes"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_elasticache_replication_group" "valkey" {
  replication_group_id = "${local.name}-valkey"
  description          = "Valkey cluster for crawlrs frontier + politeness"
  engine               = "valkey"
  engine_version       = var.valkey_engine_version
  node_type            = var.valkey_node_type
  num_cache_clusters   = var.valkey_num_cache_nodes
  port                 = 6379

  subnet_group_name  = aws_elasticache_subnet_group.this.name
  security_group_ids = [aws_security_group.valkey.id]

  # Automatic failover requires 2+ nodes; dev runs 1.
  automatic_failover_enabled = var.valkey_num_cache_nodes > 1

  # At-rest encryption; in-transit optional for dev (adds ~1ms latency).
  at_rest_encryption_enabled = true
  transit_encryption_enabled = false

  # RDB-style snapshotting for the bloom filter state.
  snapshot_retention_limit = 1
  snapshot_window          = "03:00-04:00"
  maintenance_window       = "sun:04:00-sun:05:00"

  apply_immediately = true

  tags = local.tags
}

# ---------------------------------------------------------------------------
# Postgres (RDS)
# ---------------------------------------------------------------------------

module "rds" {
  source  = "terraform-aws-modules/rds/aws"
  version = "~> 6.0"

  identifier = "${local.name}-metadata"

  engine               = "postgres"
  engine_version       = var.postgres_engine_version
  family               = "postgres17"
  major_engine_version = "17"
  instance_class       = var.postgres_instance_class

  allocated_storage     = var.postgres_allocated_storage
  max_allocated_storage = var.postgres_allocated_storage * 5

  db_name  = var.postgres_db_name
  username = var.postgres_username
  port     = 5432

  # Let RDS generate the password; retrieve via output.
  manage_master_user_password = true

  # Networking
  db_subnet_group_name   = module.vpc.database_subnet_group_name
  vpc_security_group_ids = [aws_security_group.postgres.id]
  create_db_subnet_group = false
  subnet_ids             = module.vpc.private_subnets

  # Dev shape: no multi-AZ, skip final snapshot.
  multi_az               = false
  skip_final_snapshot    = true
  deletion_protection    = false
  backup_retention_period = 1

  # Performance Insights (free tier on db.t4g.medium).
  performance_insights_enabled = true

  tags = local.tags
}

# Database subnets for RDS (the VPC module can create these).
resource "aws_security_group" "postgres" {
  name_prefix = "${local.name}-pg-"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description     = "Postgres from EKS nodes"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags

  lifecycle {
    create_before_destroy = true
  }
}

# ---------------------------------------------------------------------------
# S3 (blob store for Parquet + WARC output)
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "data" {
  bucket        = "${var.s3_bucket_prefix}-${var.environment}-${var.region}"
  force_destroy = var.s3_force_destroy
  tags          = local.tags
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM role for crawlrs pods to write to S3 via IRSA (IAM Roles
# for Service Accounts). The pod's ServiceAccount gets annotated
# with this role ARN; the AWS SDK picks it up automatically.
module "s3_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name = "${local.name}-s3-writer"

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["crawlrs:crawlrs"]
    }
  }

  role_policy_arns = {
    s3 = aws_iam_policy.s3_writer.arn
  }

  tags = local.tags
}

resource "aws_iam_policy" "s3_writer" {
  name = "${local.name}-s3-writer"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*"
        ]
      }
    ]
  })
  tags = local.tags
}
