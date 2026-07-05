package metrics

import "github.com/prometheus/client_golang/prometheus"

// opLabel carries the DynamoDB operation name (GetItem / Query / BatchWriteItem /
// ...) on the backend-telemetry collectors. It is a small, fixed set.
const opLabel = "operation"

// DynamoObserver records DynamoDB call telemetry that the command-level metrics
// cannot see: SDK retry attempts (a rising attempt count is an early warning of
// throttling BEFORE retries exhaust and a command fails), throttled retries, and
// consumed read/write capacity units per operation (for capacity planning and hot-op
// attribution that table-level CloudWatch metrics do not break down).
//
// It structurally implements internal/ddbobs.Recorder — ObserveOperation and
// ObserveCapacity match that interface — without importing ddbobs, keeping the
// dependency one-way (the assembly layer passes a *DynamoObserver to
// ddbobs.WithObservability).
type DynamoObserver struct {
	attempts  *prometheus.HistogramVec
	throttled *prometheus.CounterVec
	readCU    *prometheus.CounterVec
	writeCU   *prometheus.CounterVec
}

// NewDynamoObserver builds the collectors and registers them on reg.
func NewDynamoObserver(reg *prometheus.Registry) *DynamoObserver {
	d := &DynamoObserver{
		attempts: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "dynamodb_operation_attempts",
			Help:      "SDK attempt count per DynamoDB operation (1 = no retry), labeled by operation.",
			Buckets:   []float64{1, 2, 3, 4, 5, 6, 8, 10},
		}, []string{opLabel}),
		throttled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "dynamodb_throttled_retries_total",
			Help:      "DynamoDB attempts that were retried due to throttling, labeled by operation.",
		}, []string{opLabel}),
		readCU: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "dynamodb_consumed_read_units_total",
			Help:      "DynamoDB consumed read capacity units, labeled by operation.",
		}, []string{opLabel}),
		writeCU: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "dynamodb_consumed_write_units_total",
			Help:      "DynamoDB consumed write capacity units, labeled by operation.",
		}, []string{opLabel}),
	}
	reg.MustRegister(d.attempts, d.throttled, d.readCU, d.writeCU)
	return d
}

// ObserveOperation records one completed DynamoDB operation's attempt statistics.
func (d *DynamoObserver) ObserveOperation(op string, attempts, throttled int) {
	d.attempts.WithLabelValues(op).Observe(float64(attempts))
	if throttled > 0 {
		d.throttled.WithLabelValues(op).Add(float64(throttled))
	}
}

// ObserveCapacity records the read/write capacity units an operation consumed.
func (d *DynamoObserver) ObserveCapacity(op string, read, write float64) {
	if read > 0 {
		d.readCU.WithLabelValues(op).Add(read)
	}
	if write > 0 {
		d.writeCU.WithLabelValues(op).Add(write)
	}
}
