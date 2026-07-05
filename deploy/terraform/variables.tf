variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix applied to all created resource names."
  type        = string
  default     = "redimos"
}

variable "vpc_id" {
  description = "ID of the VPC that hosts the internal NLB and Fargate tasks."
  type        = string
}

variable "private_subnet_ids" {
  description = <<-EOT
    Private subnet IDs for the internal NLB and Fargate tasks. Must span at
    least two Availability Zones so the service and NLB are multi-AZ
    (requirement 18.1). Provide one subnet per AZ you want to cover.
  EOT
  type        = list(string)

  validation {
    condition     = length(var.private_subnet_ids) >= 2
    error_message = "Provide at least two subnets in distinct AZs for multi-AZ HA (requirement 18.1)."
  }
}

variable "image_uri" {
  description = "Fully qualified container image URI for the redimos proxy (e.g. <acct>.dkr.ecr.<region>.amazonaws.com/redimos:latest)."
  type        = string
}

variable "table_name" {
  description = "DynamoDB single-table name. Matches the -table flag default in cmd/redimos."
  type        = string
  default     = "redis-data"
}

variable "desired_count" {
  description = "Number of Fargate tasks. Must be >= 2 for cross-AZ HA (requirement 18.1)."
  type        = number
  default     = 2

  validation {
    condition     = var.desired_count >= 2
    error_message = "desired_count must be at least 2 to spread tasks across AZs (requirement 18.1)."
  }
}

variable "task_cpu" {
  description = "Fargate task CPU units (e.g. 256, 512, 1024)."
  type        = number
  default     = 512
}

variable "task_memory" {
  description = "Fargate task memory in MiB (must be compatible with task_cpu)."
  type        = number
  default     = 1024
}

variable "container_port" {
  description = "TCP port the redimos proxy listens on (RESP2)."
  type        = number
  default     = 6379
}

variable "metrics_port" {
  description = "HTTP port the proxy serves /metrics and /healthz on (matches the -metrics-addr default in cmd/redimos). Scraped by the metrics pipeline; not fronted by the NLB."
  type        = number
  default     = 9121
}

variable "consistency" {
  description = "Default read consistency passed to the proxy: strong|eventual."
  type        = string
  default     = "strong"
}

variable "allowed_ingress_cidrs" {
  description = <<-EOT
    CIDR blocks allowed to reach the RESP2 port through the internal NLB.
    Restrict to client subnets/VPC CIDR - do NOT open to 0.0.0.0/0. The NLB
    scheme is internal (VPC only) per requirement 18.2.
  EOT
  type        = list(string)
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention for the proxy container logs."
  type        = number
  default     = 30
}

# -----------------------------------------------------------------------------
# CloudWatch alarm inputs (task 24.2). See alarms.tf and ../README.md.
# -----------------------------------------------------------------------------

variable "alarm_sns_topic_arns" {
  description = <<-EOT
    SNS topic ARNs to notify on ALARM/OK transitions (e.g. PagerDuty/email
    subscriptions). Empty means the alarms are created but take no action -
    wire a topic before relying on them operationally.
  EOT
  type        = list(string)
  default     = []
}

variable "alarm_period_seconds" {
  description = "Metric aggregation period (seconds) applied to all alarms."
  type        = number
  default     = 60
}

variable "throttled_requests_threshold" {
  description = "DynamoDB ThrottledRequests (sum over operations) per period that triggers the alarm. On-demand tables should see ~0 sustained throttling."
  type        = number
  default     = 1
}

variable "throttle_evaluation_periods" {
  description = "Consecutive periods ThrottledRequests must breach before alarming."
  type        = number
  default     = 5
}

variable "system_errors_threshold" {
  description = "DynamoDB SystemErrors (sum over operations) per period that triggers the alarm. Backend 5xx should be ~0."
  type        = number
  default     = 1
}

variable "system_errors_evaluation_periods" {
  description = "Consecutive periods SystemErrors must breach before alarming."
  type        = number
  default     = 5
}

variable "enable_custom_metric_alarms" {
  description = <<-EOT
    Create the proxy p99-latency and delete-queue-backlog alarms. These read a
    custom CloudWatch namespace populated by the metrics pipeline (Prometheus
    scrape -> ADOT/CloudWatch agent). Set false until that pipeline exists so
    the module applies without alarms in INSUFFICIENT_DATA.
  EOT
  type        = bool
  default     = true
}

variable "proxy_metrics_namespace" {
  description = "CloudWatch namespace the proxy's Prometheus metrics are published under by the metrics pipeline."
  type        = string
  default     = "redimos"
}

variable "p99_latency_metric_name" {
  description = "CloudWatch metric name carrying the command-latency distribution (from redimos_command_duration_seconds) that supports the p99 extended statistic."
  type        = string
  default     = "command_duration_seconds"
}

variable "p99_latency_threshold_seconds" {
  description = "p99 command-latency alarm threshold in seconds. Design budget is region-internal p99 ~10-20ms on strong reads; default alarms at 50ms."
  type        = number
  default     = 0.05
}

variable "latency_evaluation_periods" {
  description = "Consecutive periods p99 latency must breach before alarming."
  type        = number
  default     = 5
}

variable "delete_queue_backlog_metric_name" {
  description = "CloudWatch metric name for the delete-queue backlog gauge, sourced from Deleter.QueueLen() (e.g. redimos_delete_queue_backlog)."
  type        = string
  default     = "delete_queue_backlog"
}

variable "delete_queue_backlog_threshold" {
  description = "Delete-queue backlog alarm threshold (pending pks). Default is half the deleter's default queue capacity (1024) so the alarm fires well before pks are dropped to the sweeper."
  type        = number
  default     = 512
}

variable "backlog_evaluation_periods" {
  description = "Consecutive periods the delete-queue backlog must breach before alarming."
  type        = number
  default     = 5
}
