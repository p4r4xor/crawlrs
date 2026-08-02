data "aws_caller_identity" "current" {}

data "terraform_remote_state" "platform" {
  backend = "s3"

  config = {
    bucket = var.state_bucket
    key    = "aws-dev/platform/terraform.tfstate"
    region = var.region
  }
}

locals {
  platform = data.terraform_remote_state.platform.outputs
  name     = local.platform.name_prefix
}

module "valkey" {
  source = "../../../modules/valkey"

  name       = "${local.name}-valkey"
  vpc_id     = local.platform.vpc_id
  subnet_ids = local.platform.database_subnets

  allowed_security_group_ids = [local.platform.node_security_group_id]

  node_type                  = var.valkey_node_type
  num_cache_clusters         = var.valkey_num_cache_clusters
  engine_version             = var.valkey_engine_version
  transit_encryption_enabled = var.valkey_transit_encryption_enabled
  apply_immediately          = var.apply_immediately
}

module "postgres" {
  source = "../../../modules/postgres"

  name                 = "${local.name}-metadata"
  vpc_id               = local.platform.vpc_id
  db_subnet_group_name = local.platform.database_subnet_group_name

  allowed_security_group_ids = [local.platform.node_security_group_id]

  instance_class        = var.postgres_instance_class
  engine_version        = var.postgres_engine_version
  allocated_storage     = var.postgres_allocated_storage
  max_allocated_storage = var.postgres_max_allocated_storage

  multi_az                = var.postgres_multi_az
  backup_retention_period = var.postgres_backup_retention_period
  deletion_protection     = var.postgres_deletion_protection
  skip_final_snapshot     = var.postgres_skip_final_snapshot
  apply_immediately       = var.apply_immediately
}

module "object_store" {
  source = "../../../modules/object-store"

  bucket_name   = "${local.name}-data-${data.aws_caller_identity.current.account_id}"
  force_destroy = var.s3_force_destroy

  role_name         = "${local.name}-s3-writer"
  oidc_provider_arn = local.platform.oidc_provider_arn

  namespace_service_accounts = var.namespace_service_accounts

  transition_to_ia_days      = var.s3_transition_to_ia_days
  transition_to_glacier_days = var.s3_transition_to_glacier_days
  expiration_days            = var.s3_expiration_days
}
