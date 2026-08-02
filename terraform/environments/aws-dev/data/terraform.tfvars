# Dev overrides. Anything absent here takes the default from variables.tf.
# Every value below trades a safety property for cost or speed; see the
# README's "Dev shape" section.

valkey_num_cache_clusters         = 1
valkey_transit_encryption_enabled = false

postgres_multi_az                = false
postgres_backup_retention_period = 1
postgres_deletion_protection     = false
postgres_skip_final_snapshot     = true

s3_force_destroy              = true
s3_transition_to_ia_days      = 0
s3_transition_to_glacier_days = 0
s3_expiration_days            = 30

apply_immediately = true
