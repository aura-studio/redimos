# -----------------------------------------------------------------------------
# Security groups.
# The NLB is internal (requirement 18.2). Clients inside the VPC reach the
# RESP2 port; the tasks accept traffic on the container port from that CIDR set.
# -----------------------------------------------------------------------------

# SG attached to the internal NLB. NLBs support security groups; ingress is
# restricted to the allowed client CIDRs on the RESP2 port only.
resource "aws_security_group" "nlb" {
  name        = "${var.name_prefix}-nlb"
  description = "Ingress to the internal redimos NLB on the RESP2 port"
  vpc_id      = var.vpc_id

  ingress {
    description = "RESP2 from allowed client CIDRs"
    from_port   = var.container_port
    to_port     = var.container_port
    protocol    = "tcp"
    cidr_blocks = var.allowed_ingress_cidrs
  }

  egress {
    description = "Forward to Fargate tasks"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# SG attached to the Fargate tasks (awsvpc ENIs). Only the NLB SG may reach the
# container port.
resource "aws_security_group" "tasks" {
  name        = "${var.name_prefix}-tasks"
  description = "redimos Fargate tasks - RESP2 from the NLB only"
  vpc_id      = var.vpc_id

  ingress {
    description     = "RESP2 from the internal NLB"
    from_port       = var.container_port
    to_port         = var.container_port
    protocol        = "tcp"
    security_groups = [aws_security_group.nlb.id]
  }

  # Metrics scrape path (requirement 18.5): /metrics + /healthz on the metrics
  # port, reachable only from in-VPC client CIDRs (the metrics pipeline /
  # ADOT/CloudWatch agent). Not fronted by the NLB.
  ingress {
    description = "Prometheus /metrics + /healthz from in-VPC scrapers"
    from_port   = var.metrics_port
    to_port     = var.metrics_port
    protocol    = "tcp"
    cidr_blocks = var.allowed_ingress_cidrs
  }

  # Outbound to AWS APIs (DynamoDB/KMS/ECR/Logs). Prefer VPC endpoints in prod;
  # egress is left open here so the module works with or without NAT/endpoints.
  egress {
    description = "Outbound to AWS service endpoints"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
