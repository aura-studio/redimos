# -----------------------------------------------------------------------------
# ECS Fargate cluster, task definition, and service.
# Requirement 18.1: >= 2 tasks spread across >= 2 AZs (stateless proxy).
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "redimos" {
  name              = "/ecs/${var.name_prefix}"
  retention_in_days = var.log_retention_days
}

resource "aws_ecs_cluster" "this" {
  name = "${var.name_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_task_definition" "redimos" {
  family                   = var.name_prefix
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name      = "redimos"
      image     = var.image_uri
      essential = true

      command = [
        "-addr=:${var.container_port}",
        "-metrics-addr=:${var.metrics_port}",
        "-table=${var.table_name}",
        "-consistency=${var.consistency}",
      ]

      environment = [
        { name = "AWS_REGION", value = var.region }
      ]

      portMappings = [
        {
          containerPort = var.container_port
          protocol      = "tcp"
        },
        {
          # /metrics + /healthz (requirement 18.5). Reachable within the VPC for
          # the metrics scraper; deliberately NOT registered with the NLB.
          containerPort = var.metrics_port
          protocol      = "tcp"
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.redimos.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "redimos"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "redimos" {
  name            = "${var.name_prefix}-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.redimos.arn
  desired_count   = var.desired_count # >= 2 (validated in variables.tf)
  launch_type     = "FARGATE"

  # Fargate spreads tasks across the provided subnets' AZs automatically; with
  # >=2 subnets in distinct AZs and desired_count >=2 the service is multi-AZ.
  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.tasks.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.this.arn
    container_name   = "redimos"
    container_port   = var.container_port
  }

  # Keep at least half the fleet healthy during rolling deploys.
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200

  # Avoid racing the listener during first apply.
  depends_on = [aws_lb_listener.resp2]

  lifecycle {
    ignore_changes = [desired_count] # allow external autoscaling to own this
  }
}
