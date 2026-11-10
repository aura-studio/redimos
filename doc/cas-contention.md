# Conditional-write retry optimization

Base: v2.61.0-equivalent tree after reverting c49d867.

String/Hash increments and APPEND/SETRANGE share RetryCAS. First attempt is
immediate. Only conflicts retry, up to 50 attempts. Full jitter starts at 0–5ms,
doubles to a 0–100ms cap; the final failed attempt does not sleep. With 49 waits,
expected cumulative wait is 2.278s, maximum 4.555s, excluding backend requests.
The parameter set remains a benchmark candidate, not production acceptance.

INCR/HINCRBY retain Binary reads and compare the physical value, keeping writes Binary.
APPEND/SETRANGE re-read the physical value before CAS to preserve the comparison
API. Connection closure in the embedded transport cancels backend context and
future retries. TCP behavior and default total timeout are unchanged.

Cancellation cannot undo committed writes; lost responses leave results unknown.
go-redis defaults remain: 3s read timeout, 4s pool wait, 3 retries. Pool timeout
before send differs from response timeout after send. Non-idempotent command
replay may apply an increment twice. Applications should collect PoolStats and
command latency/error hooks. Full concurrency, cancellation, type compatibility
and observability acceptance is required before release. No release or deployment
is included in this change.
