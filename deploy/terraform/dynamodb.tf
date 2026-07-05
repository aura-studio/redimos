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

# Single table modeled per the redimo/v2 layout: pk (partition) + sk (sort), BOTH
# Binary (B) — the fork stores keys and members as raw bytes so binary-safe key /
# member / element names round-trip (a String-typed key would fold non-UTF-8 bytes
# to U+FFFD and collide). skN (Number) plus the "idx" local secondary index give
# sorted-set score / list-position ordering (redimo Queries the LSI in skN order).
resource "aws_dynamodb_table" "redis_data" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST" # on-demand; on-demand start per design

  hash_key  = "pk"
  range_key = "sk"

  attribute {
    name = "pk"
    type = "B"
  }

  attribute {
    name = "sk"
    type = "B"
  }

  # Numeric sort key projected into the "idx" LSI for score/position ordering.
  attribute {
    name = "skN"
    type = "N"
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

  # Local secondary index "idx" over (pk, skN): the redimo/v2 fork Queries it in
  # skN order to serve sorted-set score ranges and list positions. It is REQUIRED
  # by the storage layout — a table without it fails those reads. KEYS_ONLY keeps
  # the index cost minimal (the base item is fetched by key when needed).
  local_secondary_index {
    name            = "idx"
    range_key       = "skN"
    projection_type = "KEYS_ONLY"
  }
}
