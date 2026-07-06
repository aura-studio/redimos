package storage

import (
	"context"
	"errors"

	redimo "github.com/aura-studio/redimo/v2"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// New builds a redimo-backed Store from an AWS DynamoDB client, a table name and
// a consistency option. Construction performs no network calls.
func New(ddb *dynamodb.Client, opts Options) Store {
	c := redimo.NewClient(ddb)

	if opts.TableName != "" {
		c = c.Table(opts.TableName)
	}

	// P0 default: strongly consistent reads (read-your-writes). A caller must
	// explicitly opt out via EventuallyConsistent, so a bare Options{} is strong.
	if opts.EventuallyConsistent {
		c = c.EventuallyConsistent()
	} else {
		c = c.StronglyConsistent()
	}

	// Wrap the redimo-backed store in the throttle decorator so every operation's
	// error is classified: a DynamoDB throttle surfaces as ErrThrottled and fires
	// the OnThrottle alerting hook (requirement 18.8). Retry/backoff for throttling
	// is handled by the AWS SDK client's retryer (see throttle.go / ErrThrottled).
	base := &redimoStore{client: c, deleteBatchSize: clampBatchSize(opts.DeleteBatchSize)}
	return newThrottleStore(base, opts.OnThrottle, opts.Breaker)
}

// NewFromClient wraps an already-configured redimo.Client. Useful when the caller
// needs full control over the client (index/attribute names, transaction limits).
// Like New it applies the throttle decorator (with no alerting hook) so throttling
// errors are still surfaced as ErrThrottled for the command layer to map.
func NewFromClient(client redimo.Client) Store {
	base := &redimoStore{client: client, deleteBatchSize: redimo.MaxBatchWriteItems}
	return newThrottleStore(base, nil, nil)
}

// clampBatchSize normalizes a configured delete batch size to the DynamoDB per-call
// limit. A value <= 0 (or above the limit) selects the maximum.
func clampBatchSize(n int) int {
	if n <= 0 || n > redimo.MaxBatchWriteItems {
		return redimo.MaxBatchWriteItems
	}

	return n
}

func (s *redimoStore) EnsureType(ctx context.Context, pk, expected string, cntDelta int64) (int64, error) {
	newCount, err := s.client.WithContext(ctx).EnsureType(pk, redimo.KeyType(expected), cntDelta)
	if errors.Is(err, redimo.ErrWrongType) {
		return 0, ErrWrongType
	}

	return newCount, err
}

func (s *redimoStore) CreateTypeIfAbsent(ctx context.Context, pk, expected string, cntDelta, nowEpoch int64) (bool, error) {
	// The single conditional meta write claims a logically-absent (or expired) key
	// atomically; created=false means the key is live, which is not an error for
	// SETNX.
	return s.client.WithContext(ctx).CreateTypeIfAbsent(pk, redimo.KeyType(expected), cntDelta, nowEpoch)
}

func (s *redimoStore) LoadMeta(ctx context.Context, pk string) (Meta, bool, error) {
	m, found, err := s.client.WithContext(ctx).LoadMeta(pk)
	if err != nil || !found {
		return Meta{}, found, err
	}

	return Meta{Type: string(m.Type), Exp: m.Exp, Count: m.Count}, true, nil
}

func (s *redimoStore) SetExpire(ctx context.Context, pk string, expEpoch int64) (bool, error) {
	return s.client.WithContext(ctx).SetExpire(pk, expEpoch)
}

func (s *redimoStore) Persist(ctx context.Context, pk string) (bool, error) {
	return s.client.WithContext(ctx).Persist(pk)
}

func (s *redimoStore) DeleteMeta(ctx context.Context, pk string) (bool, error) {
	return s.client.WithContext(ctx).DeleteMeta(pk)
}

func (s *redimoStore) DeleteMetaIfEmpty(ctx context.Context, pk string) (bool, error) {
	return s.client.WithContext(ctx).DeleteMetaIfEmpty(pk)
}

func (s *redimoStore) DeleteMembers(ctx context.Context, pk string) (int, error) {
	return s.client.WithContext(ctx).DeleteMembers(pk, s.deleteBatchSize)
}

func (s *redimoStore) SweepOrphans(ctx context.Context) (int, error) {
	return s.client.WithContext(ctx).SweepOrphans(s.deleteBatchSize)
}

// casRetry runs the bounded optimistic-concurrency (compare-and-set) loop shared
// by the read-modify-write value writes — the String INCR-family reconciliation
// (IncrBy / IncrByFloat) below, and any future value RMW that lands its result
// with a conditional write. It is the storage-layer retry helper the concurrency
// design (task 20.1, requirements 15.2, 16.3, 16.4) prescribes: it does not depend
// on read consistency because the conditional write's precondition is evaluated at
// write time against the current item, so two concurrent read-modify-writes on the
// same key cannot both land with a stale base.
//
// Each iteration calls attempt, which must read the current value, compute the new
// value, and issue its conditional write (e.g. SETCAS), returning:
//
//   - ok=true  → the conditional write landed; casRetry returns nil.
//   - ok=false → the precondition failed because a concurrent writer changed the
//     value since the read (a lost race); casRetry re-invokes attempt, which
//     re-reads and recomputes on top of the winner's value.
//   - err!=nil → an unrecoverable error (a backend failure, or a value/overflow
//     validation error such as ErrNotInteger); casRetry returns it immediately
//     without further attempts.
//
// The loop is bounded by MaxRMWRetries; when every attempt loses its race it
// returns ErrRMWMaxRetries (pathological hot-key contention) rather than looping
// forever or silently dropping the write. attempt is always called at least once.
func casRetry(attempt func() (ok bool, err error)) error {
	for i := 0; i < MaxRMWRetries; i++ {
		ok, err := attempt()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		// ok=false: the conditional write lost a race with a concurrent writer.
		// Loop to re-read and recompute on top of the value that actually landed.
	}

	rmwExhausted.Add(1)

	return ErrRMWMaxRetries
}
