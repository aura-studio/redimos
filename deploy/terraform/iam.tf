# -----------------------------------------------------------------------------
# IAM roles.
#   - execution role: lets ECS/Fargate pull the image and ship logs.
#   - task role:      LEAST-PRIVILEGE application identity, scoped to the single
#                     DynamoDB table + its KMS key only (requirement 18.3).
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# ---- Execution role (infrastructure plane) ----------------------------------
resource "aws_iam_role" "execution" {
  name               = "${var.name_prefix}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

# AWS-managed policy for pulling images (ECR) and writing container logs.
resource "aws_iam_role_policy_attachment" "execution_managed" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# ---- Task role (application plane, least privilege) -------------------------
resource "aws_iam_role" "task" {
  name               = "${var.name_prefix}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

# Requirement 18.3: single-table CRUD only. No table wildcard, no admin
# (CreateTable/DeleteTable/*), scoped to this table ARN and its indexes.
data "aws_iam_policy_document" "task_dynamodb" {
  statement {
    sid    = "SingleTableCRUD"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:BatchGetItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:TransactWriteItems",
      "dynamodb:TransactGetItems",
      "dynamodb:ConditionCheckItem",
    ]
    resources = [
      aws_dynamodb_table.redis_data.arn,
      "${aws_dynamodb_table.redis_data.arn}/index/*",
    ]
  }

  # The proxy manages TTL via the "exp" attribute at the item level, but may
  # inspect/adjust the table's native TTL configuration.
  statement {
    sid    = "TableTTLManagement"
    effect = "Allow"
    actions = [
      "dynamodb:DescribeTimeToLive",
      "dynamodb:UpdateTimeToLive",
    ]
    resources = [aws_dynamodb_table.redis_data.arn]
  }

  # KMS access limited to the table's CMK, only the operations DynamoDB needs
  # for envelope encryption of items this identity reads/writes.
  statement {
    sid    = "TableKMS"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey",
    ]
    resources = [aws_kms_key.dynamodb.arn]
  }
}

resource "aws_iam_role_policy" "task_dynamodb" {
  name   = "${var.name_prefix}-dynamodb-least-privilege"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_dynamodb.json
}
