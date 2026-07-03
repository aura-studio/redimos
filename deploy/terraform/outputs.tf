output "nlb_dns_name" {
  description = "Internal NLB DNS name clients connect to on TCP:6379."
  value       = aws_lb.this.dns_name
}

output "nlb_arn" {
  description = "ARN of the internal Network Load Balancer."
  value       = aws_lb.this.arn
}

output "dynamodb_table_name" {
  description = "Name of the DynamoDB single table."
  value       = aws_dynamodb_table.redis_data.name
}

output "dynamodb_table_arn" {
  description = "ARN of the DynamoDB single table."
  value       = aws_dynamodb_table.redis_data.arn
}

output "kms_key_arn" {
  description = "ARN of the customer-managed KMS key encrypting the table."
  value       = aws_kms_key.dynamodb.arn
}

output "task_role_arn" {
  description = "ARN of the least-privilege task IAM role."
  value       = aws_iam_role.task.arn
}

output "ecs_cluster_name" {
  description = "Name of the ECS cluster running the proxy."
  value       = aws_ecs_cluster.this.name
}

output "ecs_service_name" {
  description = "Name of the ECS service running the proxy."
  value       = aws_ecs_service.redimos.name
}

output "alarm_arns" {
  description = "ARNs of the CloudWatch alarms created for the proxy/backend (task 24.2)."
  value = compact([
    aws_cloudwatch_metric_alarm.dynamodb_throttled_requests.arn,
    aws_cloudwatch_metric_alarm.dynamodb_system_errors.arn,
    try(aws_cloudwatch_metric_alarm.proxy_p99_latency[0].arn, ""),
    try(aws_cloudwatch_metric_alarm.proxy_delete_queue_backlog[0].arn, ""),
  ])
}
