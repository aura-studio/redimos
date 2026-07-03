# -----------------------------------------------------------------------------
# DynamoDB single table + customer-managed KMS key.
# Requirement 18.4: PITR enabled + KMS encryption.
# -----------------------------------------------------------------------------

# Customer-managed KMS key used for DynamoDB server-side encryption. A CMK (as
# opposed to the AWS-owned default) lets us scope decrypt permissions to the
# task role only and enables key rotation + auditability.
resource "aws_kms_key" "dynamodb" {
  description             = "${var.name_prefix} DynamoDB table encryption key"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "dynamodb" {
  name          = "alias/${var.name_prefix}-dynamodb"
  target_key_id = aws_kms_key.dynamodb.key_id
}

# Single table modeled per design: pk (partition) + sk (sort), both strings.
# key encoding is "{db}:{key}" with meta items at sk = "#meta".
resource "aws_dynamodb_table" "redis_data" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST" # on-demand; on-demand start per design

  hash_key  = "pk"
  range_key = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  # Native TTL attribute. The proxy stores expiry as epoch seconds in "exp" on
  # the meta item; DynamoDB native TTL provides eventual cleanup while the
  # proxy read path guarantees correctness independent of cleanup timing.
  ttl {
    attribute_name = "exp"
    enabled        = true
  }

  # Requirement 18.4: Point-In-Time Recovery.
  point_in_time_recovery {
    enabled = true
  }

  # Requirement 18.4: server-side encryption with a customer-managed KMS key.
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.dynamodb.arn
  }

  # No global/local secondary indexes by design. Sorted-set score ordering is
  # implemented by the redimo layout as an ordered "sk" encoding within each
  # partition (design.md: "redimo 用 sk 排序实现 score 序"), and SCAN/HSCAN/
  # SSCAN/ZSCAN page the base table / a single pk via Query. There is therefore
  # no separate "score index" GSI to provision; adding one would be unused cost.
}
