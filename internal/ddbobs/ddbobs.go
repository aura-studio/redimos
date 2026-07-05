// Package ddbobs provides AWS SDK v2 middleware that observes DynamoDB call
// telemetry which the redimos proxy cannot otherwise see: per-operation attempt
// counts, throttling, and consumed read/write capacity units.
//
// The middleware is installed via WithObservability, e.g.
//
//	client := dynamodb.NewFromConfig(cfg, ddbobs.WithObservability(rec))
//
// where rec implements Recorder. All observation is best-effort and nil-safe: an
// unexpected input/output shape is silently ignored rather than causing a panic.
package ddbobs

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
)

// Recorder receives DynamoDB call telemetry observed by the middleware. It is
// implemented by the caller. Implementations must be safe for concurrent use, as
// the middleware may be driven from many goroutines at once.
type Recorder interface {
	// ObserveOperation is called once per completed DynamoDB operation (after all
	// SDK retries). op is the operation name (e.g. "GetItem", "Query",
	// "BatchWriteItem"); attempts is the total attempt count (1 = no retry);
	// throttled is how many of those attempts were throttling errors.
	ObserveOperation(op string, attempts int, throttled int)
	// ObserveCapacity is called when a response carried ConsumedCapacity. read
	// and write are the total read/write capacity units consumed by the operation.
	ObserveCapacity(op string, read, write float64)
}

// Middleware step IDs. Exported-stable so callers can locate/replace them.
const (
	capacityRequestID  = "redimosCapacityRequest"
	operationObserveID = "redimosOperationObserve"
)

// throttleClassifier decides whether an attempt error was a throttling error,
// using the SDK's default set of throttle error codes.
var throttleClassifier = retry.IsErrorThrottles{
	retry.ThrottleErrorCode{Codes: retry.DefaultThrottleErrorCodes},
}

// WithObservability returns a dynamodb option that installs the observability
// middleware onto the client. Pass it to dynamodb.NewFromConfig:
//
//	client := dynamodb.NewFromConfig(cfg, ddbobs.WithObservability(rec))
//
// If rec is nil the returned option is a no-op.
func WithObservability(rec Recorder) func(*dynamodb.Options) {
	if rec == nil {
		return func(*dynamodb.Options) {}
	}
	return func(o *dynamodb.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return addMiddleware(stack, rec)
		})
	}
}

// addMiddleware wires both steps onto the smithy stack. It is idempotent-safe in
// the sense that duplicate registration returns an error from the stack rather
// than panicking; callers should not add it twice.
func addMiddleware(stack *middleware.Stack, rec Recorder) error {
	// Initialize step: flip ReturnConsumedCapacity=TOTAL on the request params so
	// responses carry ConsumedCapacity. Added at the front of Initialize so it
	// runs before the request is serialized.
	reqMW := middleware.InitializeMiddlewareFunc(capacityRequestID,
		func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (
			middleware.InitializeOutput, middleware.Metadata, error,
		) {
			setReturnConsumedCapacityTotal(in.Parameters)
			return next.HandleInitialize(ctx, in)
		})
	if err := stack.Initialize.Add(reqMW, middleware.Before); err != nil {
		return err
	}

	// Finalize step: after the operation (and all retries) completes, read the
	// attempt results and consumed capacity, and report them.
	obsMW := middleware.FinalizeMiddlewareFunc(operationObserveID,
		func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
			middleware.FinalizeOutput, middleware.Metadata, error,
		) {
			out, metadata, err := next.HandleFinalize(ctx, in)

			op := middleware.GetOperationName(ctx)

			attempts, throttled := attemptStats(metadata)
			rec.ObserveOperation(op, attempts, throttled)

			read, write := sumConsumedCapacity(out.Result)
			if read > 0 || write > 0 {
				rec.ObserveCapacity(op, read, write)
			}

			return out, metadata, err
		})
	// Add after existing Finalize middleware so this runs outermost on the way
	// back out; by the time next returns, retry metadata is populated.
	return stack.Finalize.Add(obsMW, middleware.After)
}

// attemptStats computes (attempts, throttled) from the retry attempt results
// carried in the operation metadata. If no attempt results are present it
// reports a single attempt with no throttling.
func attemptStats(metadata middleware.Metadata) (attempts int, throttled int) {
	results, ok := retry.GetAttemptResults(metadata)
	if !ok || len(results.Results) == 0 {
		// No retry metadata (e.g. request never reached the retry loop). Treat as
		// a single attempt.
		return 1, 0
	}
	attempts = len(results.Results)
	for _, r := range results.Results {
		if isThrottleErr(r.Err) {
			throttled++
		}
	}
	return attempts, throttled
}

// isThrottleErr reports whether err is a throttling error per the SDK's default
// throttle classification.
func isThrottleErr(err error) bool {
	if err == nil {
		return false
	}
	return throttleClassifier.IsErrorThrottle(err) == aws.TrueTernary
}

// setReturnConsumedCapacityTotal reflects on params and, if it has a settable
// exported field named "ReturnConsumedCapacity" of type
// types.ReturnConsumedCapacity, sets it to ReturnConsumedCapacityTotal so the
// response includes consumed capacity. Any other shape (including a struct
// lacking the field) is a no-op. Never panics.
func setReturnConsumedCapacityTotal(params interface{}) {
	v := reflect.ValueOf(params)
	// Expect a pointer to a struct (all dynamodb *Input types are pointers).
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return
	}
	f := v.FieldByName("ReturnConsumedCapacity")
	if !f.IsValid() || !f.CanSet() {
		return
	}
	// Guard the field type: it must be exactly types.ReturnConsumedCapacity.
	if f.Type() != reflect.TypeOf(ddbtypes.ReturnConsumedCapacityTotal) {
		return
	}
	f.Set(reflect.ValueOf(ddbtypes.ReturnConsumedCapacityTotal))
}

// sumConsumedCapacity reflects on an operation result for a field named
// "ConsumedCapacity" and totals its read/write capacity units. The field may be
// either *types.ConsumedCapacity (single-item ops) or []types.ConsumedCapacity
// (batch/transact ops). Every pointer is nil-checked. A result lacking the field
// or of an unexpected shape yields (0, 0). Never panics.
func sumConsumedCapacity(result interface{}) (read, write float64) {
	if result == nil {
		return 0, 0
	}
	v := reflect.ValueOf(result)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return 0, 0
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return 0, 0
	}
	f := v.FieldByName("ConsumedCapacity")
	if !f.IsValid() {
		return 0, 0
	}

	// Prefer type assertions over deeper reflection where possible.
	switch cc := f.Interface().(type) {
	case *ddbtypes.ConsumedCapacity:
		r, w := capacityOf(cc)
		return r, w
	case []ddbtypes.ConsumedCapacity:
		for i := range cc {
			r, w := capacityOf(&cc[i])
			read += r
			write += w
		}
		return read, write
	default:
		return 0, 0
	}
}

// capacityOf extracts read/write capacity units from a single ConsumedCapacity,
// nil-safe on both the struct pointer and the inner float pointers.
func capacityOf(cc *ddbtypes.ConsumedCapacity) (read, write float64) {
	if cc == nil {
		return 0, 0
	}
	if cc.ReadCapacityUnits != nil {
		read = *cc.ReadCapacityUnits
	}
	if cc.WriteCapacityUnits != nil {
		write = *cc.WriteCapacityUnits
	}
	return read, write
}
