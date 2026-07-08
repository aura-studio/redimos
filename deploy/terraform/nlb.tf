# -----------------------------------------------------------------------------
# Network Load Balancer (TCP passthrough) - requirement 18.2.
# Internal scheme (VPC only), TCP:6379 listener -> IP target group (Fargate
# awsvpc tasks register by ENI IP).
# -----------------------------------------------------------------------------

resource "aws_lb" "this" {
  name               = "${var.name_prefix}-nlb"
  load_balancer_type = "network"
  internal           = true # VPC-internal only (requirement 18.2)
  subnets            = var.private_subnet_ids
  security_groups    = [aws_security_group.nlb.id]

  enable_cross_zone_load_balancing = true
}

resource "aws_lb_target_group" "this" {
  name        = "${var.name_prefix}-tg"
  port        = var.container_port
  protocol    = "TCP"
  vpc_id      = var.vpc_id
  target_type = "ip" # Fargate awsvpc networking registers task ENI IPs

  health_check {
    protocol            = "TCP"
    port                = "traffic-port"
    healthy_threshold   = 3
    unhealthy_threshold = 3
    interval            = 10
  }

  # Allow connections to drain before deregistering a task.
  deregistration_delay = 30
}

resource "aws_lb_listener" "resp2" {
  load_balancer_arn = aws_lb.this.arn
  port              = var.container_port
  protocol          = "TCP" # TCP passthrough (RESP2 over TCP)

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.this.arn
  }
}
