# Dev environment defaults. Override per-deployment as needed.
region       = "us-east-1"
environment  = "dev"
cluster_name = "crawlrs-dev"

# EKS: small node group for dev.
eks_node_instance_types = ["m6i.xlarge"]
eks_node_min            = 2
eks_node_max            = 10
eks_node_desired        = 3

# Valkey: single node for dev; set to 2+ for prod multi-AZ.
valkey_node_type       = "cache.r7g.large"
valkey_num_cache_nodes = 1

# Postgres: small instance for dev.
postgres_instance_class    = "db.t4g.medium"
postgres_allocated_storage = 20

# S3: allow destroy for dev cleanup.
s3_force_destroy = true
