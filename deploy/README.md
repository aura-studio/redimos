# redimos deployment (IaC) — task 24.1

Infrastructure-as-code for running the `redimos` RESP2 proxy on AWS. This
directory produces the artifacts required by **requirement 18** of the
`redis-dynamodb-proxy` spec:

- **18.1** — stateless proxy, ≥2 Fargate tasks spread across ≥2 AZs.
- **18.2** — internal NLB with a TCP:6379 passthrough listener.
- **18.3** — Task IAM Role with least-privilege access to the single DynamoDB table.
- **18.4** — DynamoDB table with Point-In-Time Recovery (PITR) + KMS encryption.

> **This task (24.1) only authors the configuration.** Applying it to a real
> AWS account — provisioning live resources and wiring alarms — is the separate
> **manual / high-risk** step tracked as **task 24.2**. Nothing here contacts AWS.

## Layout

```
deploy/
├── README.md                     # this file
└── terraform/
    ├── versions.tf               # terraform + AWS provider constraints
    ├── variables.tf              # inputs (region, vpc, subnets, image, ...)
    ├── dynamodb.tf               # single table + CMK (PITR + KMS + TTL "exp")
    ├── iam.tf                    # execution role + least-privilege task role
    ├── security.tf               # NLB + task security groups
    ├── nlb.tf                    # internal NLB, TCP:6379 listener, IP target group
    ├── ecs.tf                    # cluster, task definition, service (>=2 tasks)
    ├── alarms.tf                 # CloudWatch alarms (task 24.2)
    ├── outputs.tf                # NLB DNS, table/kms/role ARNs, cluster/service
    └── terraform.tfvars.example  # sample inputs
```

## Container image

The proxy image is built from [`../Dockerfile`](../Dockerfile) (multi-stage Go
build → distroless static, non-root, exposes **6379** for RESP2 and **9121**
for `/metrics` + `/healthz`, requirement 18.5).

Because `redimos/go.mod` pins the fork with `replace github.com/aura-studio/redimo
=> ../redimo`, **the Docker build context must be the parent directory** that
contains both `redimos/` and `redimo/`:

```bash
# from the workspace directory that holds redimos/ and redimo/
docker build -f redimos/Dockerfile -t redimos:latest .

# tag + push to ECR (example)
docker tag redimos:latest 123456789012.dkr.ecr.us-east-1.amazonaws.com/redimos:latest
docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/redimos:latest
```

Set the pushed image URI as `image_uri` in your tfvars.

## Applying (task 24.2 — manual)

Prerequisites: Terraform >= 1.5, AWS credentials, a VPC with ≥2 private subnets
in distinct AZs, and the image pushed to a registry the task can pull from.

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # then edit values
terraform init
terraform fmt -check
terraform validate
terraform plan       # review carefully — creates live AWS resources
terraform apply      # HIGH-RISK: provisions real infrastructure (task 24.2)
```

Clients connect to the value of the `nlb_dns_name` output on TCP:6379 from
within the VPC.

### IAM required by the deployer

The identity running `terraform apply` needs permission to create/manage the
resources in this module (this is the operator's provisioning identity — it is
distinct from, and far broader than, the least-privilege **task role** the proxy
runs with at runtime). At minimum it must manage:

- **DynamoDB** — `CreateTable`/`UpdateTable`/`DescribeTable`/`DeleteTable`,
  `UpdateTimeToLive`/`DescribeTimeToLive`, `UpdateContinuousBackups` (PITR),
  and tagging.
- **KMS** — `CreateKey`/`DescribeKey`/`ScheduleKeyDeletion`,
  `CreateAlias`/`DeleteAlias`, `EnableKeyRotation`, and key policy management.
- **IAM** — `CreateRole`/`DeleteRole`, `PutRolePolicy`/`DeleteRolePolicy`,
  `AttachRolePolicy`/`DetachRolePolicy`, `PassRole` (to pass the execution and
  task roles to ECS).
- **ECS** — cluster, task definition, and service management.
- **ELBv2** — NLB, target group, and listener management.
- **EC2** — security group management in the target VPC.
- **CloudWatch Logs** — log group management.
- **CloudWatch** — `PutMetricAlarm`/`DeleteAlarms`/`DescribeAlarms` for task 24.2.

Scope these to the target account/region and prefer a dedicated deploy role over
long-lived user credentials. Nothing in task 24.1 uses these — provisioning is
the manual step (24.2).

## Security posture

- **Least-privilege IAM (18.3):** the task role grants only single-table CRUD
  actions (`GetItem/PutItem/UpdateItem/DeleteItem/Query/Scan/BatchGetItem/
  BatchWriteItem/TransactWriteItems/TransactGetItems/ConditionCheckItem`) scoped
  to the table ARN + its indexes, plus `DescribeTimeToLive/UpdateTimeToLive` on
  the table and `kms:Decrypt/GenerateDataKey` on the table's CMK. No table
  wildcard, no `CreateTable/DeleteTable`, no admin.
- **Encryption at rest + recovery (18.4):** the table uses a customer-managed
  KMS key (rotation enabled) and has PITR enabled.
- **Internal exposure (18.2):** the NLB scheme is `internal` (VPC only). Ingress
  is restricted to `allowed_ingress_cidrs` — do not open to `0.0.0.0/0`. Tasks
  accept RESP2 only from the NLB security group.
- **Runtime:** the container runs as a non-root user from a distroless image
  (no shell, minimal attack surface).

## Notes / follow-ups (task 24.2)

- **Client auth:** the proxy supports `requirepass`. Inject it as a secret
  (SSM Parameter Store / Secrets Manager) via the task definition rather than a
  plaintext flag; wiring is deferred to the manual step.
- **Autoscaling:** `desired_count` changes are ignored by the service lifecycle
  so an Application Auto Scaling target-tracking policy (connections + p99) can
  own scaling. Define that policy during 24.2.
- **VPC endpoints:** prefer interface/gateway VPC endpoints for DynamoDB, KMS,
  ECR, and CloudWatch Logs to keep traffic off the public internet.
- **Alarms:** authored in [`terraform/alarms.tf`](terraform/alarms.tf); wiring
  actions/thresholds is the 24.2 step. See the runbook below.

## CloudWatch alarms runbook (task 24.2)

Four alarms are defined in [`terraform/alarms.tf`](terraform/alarms.tf) and are
created by `terraform apply`. Tune them with the variables in
[`terraform/variables.tf`](terraform/variables.tf) and route them by setting
`alarm_sns_topic_arns` to an SNS topic your on-call subscribes to (empty = alarm
created but no notification action).

1. **DynamoDB `ThrottledRequests`** (`<name_prefix>-dynamodb-throttled-requests`)
   — native `AWS/DynamoDB` metric summed over all operations for the table via a
   `SEARCH` expression. Sustained throttling means the proxy is returning
   retryable `-ERR` to clients (req 18.8): raise capacity / investigate a hot
   key. Tune with `throttled_requests_threshold` and
   `throttle_evaluation_periods`.

2. **DynamoDB `SystemErrors`** (`<name_prefix>-dynamodb-system-errors`) — native
   `AWS/DynamoDB` backend-5xx metric summed over operations. Non-zero indicates
   backend faults (not client error). Tune with `system_errors_threshold` and
   `system_errors_evaluation_periods`.

3. **Proxy p99 latency** (`<name_prefix>-proxy-p99-latency`) — reads the
   proxy's command-latency distribution (Prometheus
   `redimos_command_duration_seconds`, served on **:9121/metrics**) using the
   `p99` extended statistic. Design budget is region-internal p99 ~10–20 ms on
   strong reads; default alarm at 50 ms. Tune with
   `p99_latency_threshold_seconds` and `latency_evaluation_periods`.

4. **Delete-queue backlog** (`<name_prefix>-proxy-delete-queue-backlog`) —
   reads the lazy-delete backlog gauge sourced from `Deleter.QueueLen()`. A
   persistently high backlog means the background deleter is falling behind and
   pks risk being dropped to the weekly sweeper. Default threshold is 512 (half
   the deleter's 1024 default capacity). Tune with
   `delete_queue_backlog_threshold` and `backlog_evaluation_periods`.

Alarms 1–2 use native DynamoDB metrics and are always created. Alarms 3–4 read
a **custom** CloudWatch namespace that must be populated by a metrics pipeline
(scrape `:9121/metrics` with an ADOT/CloudWatch agent and publish to the
`proxy_metrics_namespace`). They are gated behind `enable_custom_metric_alarms`
— leave it `false` until that pipeline exists so they don't sit in
`INSUFFICIENT_DATA`. Override the metric names with `p99_latency_metric_name`
and `delete_queue_backlog_metric_name` to match what your pipeline publishes.
