# -----------------------------------------------------------------------------
# CloudWatch alarms (task 24.2).
#
# These codify the alarms the design's "部署与运维 / 告警" section and
# requirement 18.4 call for so they are reviewable in version control:
#
#   * DynamoDB ThrottledRequests  (backend throttling, AWS/DynamoDB)
#   * DynamoDB SystemErrors        (backend 5xx, AWS/DynamoDB)
#   * proxy p99 command latency    (custom namespace, from Prometheus)
#   * delete-queue backlog         (custom namespace, from Prometheus)
#
# NOTE: authoring only. Applying these (like the rest of the module) is the
# manual, account-owner step described in RUNBOOK.md. Nothing here contacts AWS.
#
# The two DynamoDB alarms use native AWS/DynamoDB metrics and are always
# created. The two proxy alarms read a custom namespace that must be populated
# by the metrics pipeline (Prometheus scrape -> ADOT/CloudWatch agent). They are
# gated behind var.enable_custom_metric_alarms so the module still applies
# cleanly before that pipeline exists.
# -----------------------------------------------------------------------------

# --- DynamoDB: ThrottledRequests ---------------------------------------------
# ThrottledRequests is emitted per (TableName, Operation). We sum across all
# operations for this table with a metric-math SEARCH so a single alarm covers
# read + write + transactional throttling. Any sustained throttling means the
# proxy is propagating -ERR (retryable) to clients (requirement 18.8) and the
# table needs more capacity or a hotter key needs attention.
resource "aws_cloudwatch_metric_alarm" "dynamodb_throttled_requests" {
  alarm_name          = "${var.name_prefix}-dynamodb-throttled-requests"
  alarm_description   = "DynamoDB ThrottledRequests (summed over operations) for ${var.table_name}. Sustained throttling => proxy returns retryable -ERR to clients (req 18.8)."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = var.throttle_evaluation_periods
  threshold           = var.throttled_requests_threshold
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns

  metric_query {
    id          = "throttled_sum"
    expression  = "SUM(SEARCH('{AWS/DynamoDB,Operation,TableName} MetricName=\"ThrottledRequests\" TableName=\"${var.table_name}\"', 'Sum', ${var.alarm_period_seconds}))"
    label       = "ThrottledRequests (all operations)"
    return_data = true
  }
}

# --- DynamoDB: SystemErrors ---------------------------------------------------
# SystemErrors counts DynamoDB-side 5xx (HTTP 500) responses per
# (TableName, Operation). Any non-zero rate indicates backend faults, not
# client error; summed across operations for the table.
resource "aws_cloudwatch_metric_alarm" "dynamodb_system_errors" {
  alarm_name          = "${var.name_prefix}-dynamodb-system-errors"
  alarm_description   = "DynamoDB SystemErrors (summed over operations) for ${var.table_name}. Non-zero => backend faults."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = var.system_errors_evaluation_periods
  threshold           = var.system_errors_threshold
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns

  metric_query {
    id          = "system_errors_sum"
    expression  = "SUM(SEARCH('{AWS/DynamoDB,Operation,TableName} MetricName=\"SystemErrors\" TableName=\"${var.table_name}\"', 'Sum', ${var.alarm_period_seconds}))"
    label       = "SystemErrors (all operations)"
    return_data = true
  }
}

# --- Proxy: p99 command latency ----------------------------------------------
# The proxy exports the Prometheus histogram redimos_command_duration_seconds.
# When published to CloudWatch (ADOT/CloudWatch agent) as a metric that carries
# a distribution, the p99 extended statistic tracks the design's latency budget
# (region-internal p99 ~10-20ms on strong reads). Alarm fires when p99 exceeds
# the budget for a sustained window.
resource "aws_cloudwatch_metric_alarm" "proxy_p99_latency" {
  count = var.enable_custom_metric_alarms ? 1 : 0

  alarm_name          = "${var.name_prefix}-proxy-p99-latency"
  alarm_description   = "redimos command p99 latency above ${var.p99_latency_threshold_seconds}s (design budget ~10-20ms strong reads). Source: ${var.proxy_metrics_namespace}/${var.p99_latency_metric_name}."
  namespace           = var.proxy_metrics_namespace
  metric_name         = var.p99_latency_metric_name
  extended_statistic  = "p99"
  period              = var.alarm_period_seconds
  evaluation_periods  = var.latency_evaluation_periods
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.p99_latency_threshold_seconds
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
}

# --- Proxy: delete-queue backlog ---------------------------------------------
# The lazy-delete queue (internal/meta/deleter.go) is bounded; a persistently
# high backlog means the background deleter is falling behind (or DynamoDB is
# slow) and pks are at risk of being dropped to the weekly sweeper. Alarm on a
# published gauge sourced from Deleter.QueueLen().
resource "aws_cloudwatch_metric_alarm" "proxy_delete_queue_backlog" {
  count = var.enable_custom_metric_alarms ? 1 : 0

  alarm_name          = "${var.name_prefix}-proxy-delete-queue-backlog"
  alarm_description   = "redimos delete-queue backlog above ${var.delete_queue_backlog_threshold}. Deleter is falling behind; risk of dropped pks handed to the weekly sweeper. Source: ${var.proxy_metrics_namespace}/${var.delete_queue_backlog_metric_name}."
  namespace           = var.proxy_metrics_namespace
  metric_name         = var.delete_queue_backlog_metric_name
  statistic           = "Maximum"
  period              = var.alarm_period_seconds
  evaluation_periods  = var.backlog_evaluation_periods
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.delete_queue_backlog_threshold
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
}
