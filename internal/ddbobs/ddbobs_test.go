package ddbobs

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
)

// capture is a Recorder that records calls for assertion.
type capture struct {
	opCalls  []opCall
	capCalls []capCall
}

type opCall struct {
	op        string
	attempts  int
	throttled int
}

type capCall struct {
	op    string
	read  float64
	write float64
}

func (c *capture) ObserveOperation(op string, attempts, throttled int) {
	c.opCalls = append(c.opCalls, opCall{op, attempts, throttled})
}

func (c *capture) ObserveCapacity(op string, read, write float64) {
	c.capCalls = append(c.capCalls, capCall{op, read, write})
}

func f64(v float64) *float64 { return &v }

// terminalHandler returns a canned output and metadata regardless of input.
type terminalHandler struct {
	result   interface{}
	metadata middleware.Metadata
}

func (t terminalHandler) Handle(ctx context.Context, in interface{}) (interface{}, middleware.Metadata, error) {
	return t.result, t.metadata, nil
}

// buildStack constructs a minimal stack with the ddbobs middleware installed and
// a terminal handler returning the given result.
func buildStack(t *testing.T, rec Recorder, result interface{}) (*middleware.Stack, middleware.Handler) {
	t.Helper()
	stack := middleware.NewStack("test", func() interface{} { return nil })
	if err := addMiddleware(stack, rec); err != nil {
		t.Fatalf("addMiddleware: %v", err)
	}
	// Mimic the SDK's deserializer: promote the terminal handler's raw response
	// into the typed Result so it reaches the Finalize step (the innermost wrap
	// handler only sets RawResponse, leaving Result nil otherwise).
	deser := middleware.DeserializeMiddlewareFunc("promoteResult",
		func(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (
			middleware.DeserializeOutput, middleware.Metadata, error,
		) {
			out, md, err := next.HandleDeserialize(ctx, in)
			if out.Result == nil {
				out.Result = out.RawResponse
			}
			return out, md, err
		})
	if err := stack.Deserialize.Add(deser, middleware.After); err != nil {
		t.Fatalf("add deserialize: %v", err)
	}
	term := terminalHandler{result: result}
	handler := middleware.DecorateHandler(term,
		stack.Initialize,
		stack.Serialize,
		stack.Build,
		stack.Finalize,
		stack.Deserialize,
	)
	return stack, handler
}

func TestObserveOperationAndCapacity(t *testing.T) {
	rec := &capture{}

	out := &dynamodb.GetItemOutput{
		ConsumedCapacity: &ddbtypes.ConsumedCapacity{
			ReadCapacityUnits:  f64(3.5),
			WriteCapacityUnits: f64(0),
		},
	}

	_, handler := buildStack(t, rec, out)

	// Set the operation name on the context the way the SDK does.
	ctx := middleware.WithOperationName(context.Background(), "GetItem")

	in := &dynamodb.GetItemInput{}
	if _, _, err := handler.Handle(ctx, in); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// The request middleware should have flipped ReturnConsumedCapacity on the input.
	if in.ReturnConsumedCapacity != ddbtypes.ReturnConsumedCapacityTotal {
		t.Errorf("ReturnConsumedCapacity = %q, want %q",
			in.ReturnConsumedCapacity, ddbtypes.ReturnConsumedCapacityTotal)
	}

	// ObserveOperation should fire once with op=GetItem, attempts=1, throttled=0
	// (no retry metadata present -> single attempt).
	if len(rec.opCalls) != 1 {
		t.Fatalf("opCalls = %d, want 1", len(rec.opCalls))
	}
	if got := rec.opCalls[0]; got.op != "GetItem" || got.attempts != 1 || got.throttled != 0 {
		t.Errorf("opCall = %+v, want {GetItem 1 0}", got)
	}

	// ObserveCapacity should fire once with read=3.5, write=0.
	if len(rec.capCalls) != 1 {
		t.Fatalf("capCalls = %d, want 1", len(rec.capCalls))
	}
	if got := rec.capCalls[0]; got.op != "GetItem" || got.read != 3.5 || got.write != 0 {
		t.Errorf("capCall = %+v, want {GetItem 3.5 0}", got)
	}
}

func TestObserveCapacitySkippedWhenZero(t *testing.T) {
	rec := &capture{}

	// Output with no ConsumedCapacity at all.
	out := &dynamodb.GetItemOutput{}
	_, handler := buildStack(t, rec, out)

	ctx := middleware.WithOperationName(context.Background(), "GetItem")
	if _, _, err := handler.Handle(ctx, &dynamodb.GetItemInput{}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// ObserveOperation still fires; ObserveCapacity must NOT (capacity == 0).
	if len(rec.opCalls) != 1 {
		t.Fatalf("opCalls = %d, want 1", len(rec.opCalls))
	}
	if len(rec.capCalls) != 0 {
		t.Fatalf("capCalls = %d, want 0", len(rec.capCalls))
	}
}

func TestWithObservabilityNilRecorderIsNoOp(t *testing.T) {
	// Should not panic and should produce a usable (empty-effect) option.
	opt := WithObservability(nil)
	o := &dynamodb.Options{}
	before := len(o.APIOptions)
	opt(o)
	if len(o.APIOptions) != before {
		t.Errorf("nil recorder appended %d APIOptions, want 0", len(o.APIOptions)-before)
	}
}

func TestWithObservabilityAppendsAPIOption(t *testing.T) {
	rec := &capture{}
	opt := WithObservability(rec)
	o := &dynamodb.Options{}
	opt(o)
	if len(o.APIOptions) != 1 {
		t.Fatalf("APIOptions = %d, want 1", len(o.APIOptions))
	}
	// The appended func must successfully wire onto a fresh stack.
	stack := middleware.NewStack("test", func() interface{} { return nil })
	if err := o.APIOptions[0](stack); err != nil {
		t.Fatalf("APIOption(stack): %v", err)
	}
	ids := stack.Initialize.List()
	if !contains(ids, capacityRequestID) {
		t.Errorf("Initialize step missing %q; got %v", capacityRequestID, ids)
	}
	if !contains(stack.Finalize.List(), operationObserveID) {
		t.Errorf("Finalize step missing %q; got %v", operationObserveID, stack.Finalize.List())
	}
}

// --- reflection helper tests ---

func TestSetReturnConsumedCapacityTotal(t *testing.T) {
	in := &dynamodb.GetItemInput{}
	setReturnConsumedCapacityTotal(in)
	if in.ReturnConsumedCapacity != ddbtypes.ReturnConsumedCapacityTotal {
		t.Errorf("got %q, want %q", in.ReturnConsumedCapacity, ddbtypes.ReturnConsumedCapacityTotal)
	}
}

func TestSetReturnConsumedCapacityTotalNoField(t *testing.T) {
	// A struct without the field must be a no-op (and must not panic).
	type noField struct{ Name string }
	x := &noField{Name: "x"}
	setReturnConsumedCapacityTotal(x) // should do nothing
	if x.Name != "x" {
		t.Errorf("unexpected mutation: %+v", x)
	}
}

func TestSetReturnConsumedCapacityWrongTypes(t *testing.T) {
	// Field present but wrong type must be a no-op.
	type wrongType struct{ ReturnConsumedCapacity string }
	w := &wrongType{}
	setReturnConsumedCapacityTotal(w)
	if w.ReturnConsumedCapacity != "" {
		t.Errorf("wrong-typed field mutated: %q", w.ReturnConsumedCapacity)
	}

	// Non-pointer, nil, and nil-typed inputs must not panic.
	setReturnConsumedCapacityTotal(nil)
	setReturnConsumedCapacityTotal(dynamodb.GetItemInput{}) // value, not pointer
	var np *dynamodb.GetItemInput
	setReturnConsumedCapacityTotal(np) // typed nil pointer
	setReturnConsumedCapacityTotal(42)
}

func TestSumConsumedCapacitySingle(t *testing.T) {
	out := &dynamodb.GetItemOutput{
		ConsumedCapacity: &ddbtypes.ConsumedCapacity{
			ReadCapacityUnits:  f64(2),
			WriteCapacityUnits: f64(1),
		},
	}
	r, w := sumConsumedCapacity(out)
	if r != 2 || w != 1 {
		t.Errorf("got (%v,%v), want (2,1)", r, w)
	}
}

func TestSumConsumedCapacitySingleNilInnerPointers(t *testing.T) {
	// ConsumedCapacity present but ReadCapacityUnits/WriteCapacityUnits nil.
	out := &dynamodb.GetItemOutput{ConsumedCapacity: &ddbtypes.ConsumedCapacity{}}
	r, w := sumConsumedCapacity(out)
	if r != 0 || w != 0 {
		t.Errorf("got (%v,%v), want (0,0)", r, w)
	}
}

func TestSumConsumedCapacitySlice(t *testing.T) {
	out := &dynamodb.BatchWriteItemOutput{
		ConsumedCapacity: []ddbtypes.ConsumedCapacity{
			{ReadCapacityUnits: f64(1), WriteCapacityUnits: f64(2)},
			{ReadCapacityUnits: f64(3), WriteCapacityUnits: nil}, // nil write pointer
			{}, // both nil
		},
	}
	r, w := sumConsumedCapacity(out)
	if r != 4 || w != 2 {
		t.Errorf("got (%v,%v), want (4,2)", r, w)
	}
}

func TestSumConsumedCapacityNilAndMissing(t *testing.T) {
	// nil interface.
	if r, w := sumConsumedCapacity(nil); r != 0 || w != 0 {
		t.Errorf("nil: got (%v,%v), want (0,0)", r, w)
	}
	// Nil ConsumedCapacity pointer field.
	if r, w := sumConsumedCapacity(&dynamodb.GetItemOutput{}); r != 0 || w != 0 {
		t.Errorf("nil field: got (%v,%v), want (0,0)", r, w)
	}
	// Struct without a ConsumedCapacity field at all.
	type noCap struct{ X int }
	if r, w := sumConsumedCapacity(&noCap{X: 5}); r != 0 || w != 0 {
		t.Errorf("missing field: got (%v,%v), want (0,0)", r, w)
	}
	// Typed nil pointer.
	var np *dynamodb.GetItemOutput
	if r, w := sumConsumedCapacity(np); r != 0 || w != 0 {
		t.Errorf("typed nil: got (%v,%v), want (0,0)", r, w)
	}
	// Non-struct.
	if r, w := sumConsumedCapacity(42); r != 0 || w != 0 {
		t.Errorf("int: got (%v,%v), want (0,0)", r, w)
	}
}

func TestIsThrottleErr(t *testing.T) {
	// A DynamoDB throttle error code should classify as throttling.
	throttle := &smithy.GenericAPIError{Code: "ProvisionedThroughputExceededException"}
	if !isThrottleErr(throttle) {
		t.Errorf("ProvisionedThroughputExceededException not classified as throttle")
	}
	// A generic throttling code.
	if !isThrottleErr(&smithy.GenericAPIError{Code: "ThrottlingException"}) {
		t.Errorf("ThrottlingException not classified as throttle")
	}
	// A non-throttle API error.
	if isThrottleErr(&smithy.GenericAPIError{Code: "ValidationException"}) {
		t.Errorf("ValidationException wrongly classified as throttle")
	}
	// nil error.
	if isThrottleErr(nil) {
		t.Errorf("nil wrongly classified as throttle")
	}
}

func TestAttemptStatsEmptyMetadata(t *testing.T) {
	// No attempt results in metadata -> single attempt, no throttling.
	attempts, throttled := attemptStats(middleware.Metadata{})
	if attempts != 1 || throttled != 0 {
		t.Errorf("got (%d,%d), want (1,0)", attempts, throttled)
	}
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
