package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	smithy "github.com/aws/smithy-go"
)

// ErrThrottled is the storage-seam sentinel for a DynamoDB throttling condition
// (a ProvisionedThroughputExceededException, or a throttling APIError such as
// ThrottlingException / RequestLimitExceeded) that survived the AWS SDK's bounded
// retry/backoff. It lets the command layer detect the condition with errors.Is
// without importing the AWS SDK types, and map it to the retryable RESP reply
// "-ERR backend throttled, retry later" (see resp.ErrBackendThrottled). The
// throttle handling is applied uniformly at this seam by the throttleStore
// decorator that New / NewFromClient wrap around the redimo-backed Store, so every
// operation that is throttled surfaces ErrThrottled regardless of which command
// issued it. Requirement 18.8.
//
// Retry / backoff decision: the proxy does NOT add its own retry loop for
// throttling. The AWS SDK v2 client the Store is built on already retries
// throttling errors — ProvisionedThroughputExceededException is on the SDK's
// throttle-error list — with exponential backoff and jitter, bounded by the
// retryer's max attempts (the standard retryer's default is 3). The store
// receives an already-constructed *dynamodb.Client, so tuning the bound/backoff
// (or wiring a custom aws.Retryer) is a client-construction concern owned by the
// assembly step (task 23.1); this seam only classifies the error the SDK
// ultimately surfaces after its retries are exhausted and turns it into
// ErrThrottled (firing the alerting hook as it does so).
var ErrThrottled = errors.New("backend throttled")

// IsThrottled reports whether err (or any error it wraps) is a DynamoDB
// throttling error. It recognises the typed
// *types.ProvisionedThroughputExceededException named by requirement 18.8, and —
// via the smithy APIError interface — the throttling error CODES DynamoDB may
// otherwise surface (ThrottlingException / RequestLimitExceeded /
// TooManyRequestsException), so a throttle is detected whether the SDK returns
// the modelled exception struct or a generic API error. It is exported so callers
// outside this package can classify a raw backend error, but the command path
// relies on errors.Is(err, ErrThrottled) against the sentinel the decorator maps.
func IsThrottled(err error) bool {
	if err == nil {
		return false
	}

	// Already mapped to the storage sentinel (e.g. re-classifying a wrapped error).
	if errors.Is(err, ErrThrottled) {
		return true
	}

	// The DynamoDB-specific typed exception requirement 18.8 names explicitly.
	var pte *types.ProvisionedThroughputExceededException
	if errors.As(err, &pte) {
		return true
	}

	// Any throttling API error, matched by its service error code so it also
	// covers ThrottlingException / RequestLimitExceeded surfaced generically.
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "ProvisionedThroughputExceededException",
			"ThrottlingException",
			"Throttling",
			"ThrottledException",
			"RequestLimitExceeded",
			"TooManyRequestsException":
			return true
		}
	}

	return false
}

// throttleStore decorates a Store so every operation's error is inspected for a
// DynamoDB throttle. On a throttle it fires the injected alerting hook (if any)
// and returns ErrThrottled wrapping the original backend error; every other error
// (and every success) passes through untouched. This keeps the throttle handling
// in exactly one place instead of threading it through the redimo-backed Store's
// ~50 methods, and keeps the storage package decoupled from metrics/command — the
// hook is a plain func the assembly step injects.
type throttleStore struct {
	inner      Store
	onThrottle func()
}

// newThrottleStore wraps inner so throttling errors are classified as ErrThrottled
// and onThrottle (when non-nil) is invoked on each observed throttle.
func newThrottleStore(inner Store, onThrottle func()) *throttleStore {
	return &throttleStore{inner: inner, onThrottle: onThrottle}
}

var _ Store = (*throttleStore)(nil)

// obs is the single choke point: it classifies err, fires the alert hook on a
// throttle, and returns ErrThrottled (wrapping the original so logs keep the
// backend detail) or the original error unchanged.
func (t *throttleStore) obs(err error) error {
	if err == nil {
		return nil
	}
	if IsThrottled(err) {
		if t.onThrottle != nil {
			t.onThrottle()
		}
		return fmt.Errorf("%w: %s", ErrThrottled, err.Error())
	}

	return err
}

// --- delegating methods (each forwards to inner and routes the error through obs) ---

func (t *throttleStore) EnsureType(ctx context.Context, pk, expected string, cntDelta int64) (int64, error) {
	newCount, err := t.inner.EnsureType(ctx, pk, expected, cntDelta)
	return newCount, t.obs(err)
}

func (t *throttleStore) CreateTypeIfAbsent(ctx context.Context, pk, expected string, cntDelta, nowEpoch int64) (bool, error) {
	created, err := t.inner.CreateTypeIfAbsent(ctx, pk, expected, cntDelta, nowEpoch)
	return created, t.obs(err)
}

func (t *throttleStore) LoadMeta(ctx context.Context, pk string) (Meta, bool, error) {
	m, found, err := t.inner.LoadMeta(ctx, pk)
	return m, found, t.obs(err)
}

func (t *throttleStore) SetExpire(ctx context.Context, pk string, expEpoch int64) (bool, error) {
	found, err := t.inner.SetExpire(ctx, pk, expEpoch)
	return found, t.obs(err)
}

func (t *throttleStore) Persist(ctx context.Context, pk string) (bool, error) {
	found, err := t.inner.Persist(ctx, pk)
	return found, t.obs(err)
}

func (t *throttleStore) DeleteMeta(ctx context.Context, pk string) (bool, error) {
	existed, err := t.inner.DeleteMeta(ctx, pk)
	return existed, t.obs(err)
}

func (t *throttleStore) DeleteMetaIfEmpty(ctx context.Context, pk string) (bool, error) {
	deleted, err := t.inner.DeleteMetaIfEmpty(ctx, pk)
	return deleted, t.obs(err)
}

func (t *throttleStore) DeleteMembers(ctx context.Context, pk string) (int, error) {
	deleted, err := t.inner.DeleteMembers(ctx, pk)
	return deleted, t.obs(err)
}

func (t *throttleStore) DeleteMembersIfDead(ctx context.Context, pk string) (int, bool, error) {
	deleted, aborted, err := t.inner.DeleteMembersIfDead(ctx, pk)
	return deleted, aborted, t.obs(err)
}

func (t *throttleStore) SweepOrphans(ctx context.Context) (int, error) {
	reclaimed, err := t.inner.SweepOrphans(ctx)
	return reclaimed, t.obs(err)
}

func (t *throttleStore) GetString(ctx context.Context, pk string) ([]byte, bool, error) {
	val, found, err := t.inner.GetString(ctx, pk)
	return val, found, t.obs(err)
}

func (t *throttleStore) MGetStrings(ctx context.Context, pks []string) (map[string][]byte, error) {
	vals, err := t.inner.MGetStrings(ctx, pks)
	return vals, t.obs(err)
}

func (t *throttleStore) SetString(ctx context.Context, pk string, val []byte) error {
	return t.obs(t.inner.SetString(ctx, pk, val))
}

func (t *throttleStore) GetSetString(ctx context.Context, pk string, val []byte) ([]byte, bool, error) {
	old, existed, err := t.inner.GetSetString(ctx, pk, val)
	return old, existed, t.obs(err)
}

func (t *throttleStore) SetStringIfEquals(ctx context.Context, pk string, newVal, oldVal []byte, oldExists bool) (bool, error) {
	ok, err := t.inner.SetStringIfEquals(ctx, pk, newVal, oldVal, oldExists)
	return ok, t.obs(err)
}

func (t *throttleStore) IncrBy(ctx context.Context, pk string, delta int64) (int64, error) {
	newVal, err := t.inner.IncrBy(ctx, pk, delta)
	return newVal, t.obs(err)
}

func (t *throttleStore) IncrByFloat(ctx context.Context, pk string, delta float64) ([]byte, error) {
	newVal, err := t.inner.IncrByFloat(ctx, pk, delta)
	return newVal, t.obs(err)
}

func (t *throttleStore) HSet(ctx context.Context, pk string, fields []HField) (int, error) {
	added, err := t.inner.HSet(ctx, pk, fields)
	return added, t.obs(err)
}

func (t *throttleStore) HSetNX(ctx context.Context, pk, field string, val []byte) (bool, error) {
	set, err := t.inner.HSetNX(ctx, pk, field, val)
	return set, t.obs(err)
}

func (t *throttleStore) HGet(ctx context.Context, pk, field string) ([]byte, bool, error) {
	val, found, err := t.inner.HGet(ctx, pk, field)
	return val, found, t.obs(err)
}

func (t *throttleStore) HMGet(ctx context.Context, pk string, fields []string) (map[string][]byte, error) {
	vals, err := t.inner.HMGet(ctx, pk, fields)
	return vals, t.obs(err)
}

func (t *throttleStore) HGetAll(ctx context.Context, pk string) ([]HField, error) {
	fields, err := t.inner.HGetAll(ctx, pk)
	return fields, t.obs(err)
}

func (t *throttleStore) HKeys(ctx context.Context, pk string) ([]string, error) {
	fields, err := t.inner.HKeys(ctx, pk)
	return fields, t.obs(err)
}

func (t *throttleStore) HVals(ctx context.Context, pk string) ([][]byte, error) {
	vals, err := t.inner.HVals(ctx, pk)
	return vals, t.obs(err)
}

func (t *throttleStore) HDel(ctx context.Context, pk string, fields []string) (int, error) {
	removed, err := t.inner.HDel(ctx, pk, fields)
	return removed, t.obs(err)
}

func (t *throttleStore) HExists(ctx context.Context, pk, field string) (bool, error) {
	exists, err := t.inner.HExists(ctx, pk, field)
	return exists, t.obs(err)
}

func (t *throttleStore) HStrlen(ctx context.Context, pk, field string) (int, error) {
	length, err := t.inner.HStrlen(ctx, pk, field)
	return length, t.obs(err)
}

func (t *throttleStore) HIncrBy(ctx context.Context, pk, field string, delta int64) (int64, bool, error) {
	newVal, isNew, err := t.inner.HIncrBy(ctx, pk, field, delta)
	return newVal, isNew, t.obs(err)
}

func (t *throttleStore) HIncrByFloat(ctx context.Context, pk, field string, delta float64) ([]byte, bool, error) {
	newVal, isNew, err := t.inner.HIncrByFloat(ctx, pk, field, delta)
	return newVal, isNew, t.obs(err)
}

func (t *throttleStore) SAdd(ctx context.Context, pk string, members []string) (int, error) {
	added, err := t.inner.SAdd(ctx, pk, members)
	return added, t.obs(err)
}

func (t *throttleStore) SRem(ctx context.Context, pk string, members []string) (int, error) {
	removed, err := t.inner.SRem(ctx, pk, members)
	return removed, t.obs(err)
}

func (t *throttleStore) SIsMember(ctx context.Context, pk, member string) (bool, error) {
	isMember, err := t.inner.SIsMember(ctx, pk, member)
	return isMember, t.obs(err)
}

func (t *throttleStore) SMembers(ctx context.Context, pk string) ([]string, error) {
	members, err := t.inner.SMembers(ctx, pk)
	return members, t.obs(err)
}

func (t *throttleStore) SPop(ctx context.Context, pk string, count int) ([]string, error) {
	members, err := t.inner.SPop(ctx, pk, count)
	return members, t.obs(err)
}

func (t *throttleStore) SRandMember(ctx context.Context, pk string, count int) ([]string, error) {
	members, err := t.inner.SRandMember(ctx, pk, count)
	return members, t.obs(err)
}

func (t *throttleStore) SScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]string, map[string]types.AttributeValue, error) {
	members, nextLEK, err := t.inner.SScan(ctx, pk, lek, limit)
	return members, nextLEK, t.obs(err)
}

func (t *throttleStore) ZAdd(ctx context.Context, pk string, members []ZMember) (int, error) {
	added, err := t.inner.ZAdd(ctx, pk, members)
	return added, t.obs(err)
}

func (t *throttleStore) ZRem(ctx context.Context, pk string, members []string) (int, error) {
	removed, err := t.inner.ZRem(ctx, pk, members)
	return removed, t.obs(err)
}

func (t *throttleStore) ZScore(ctx context.Context, pk, member string) (float64, bool, error) {
	score, found, err := t.inner.ZScore(ctx, pk, member)
	return score, found, t.obs(err)
}

func (t *throttleStore) ZIncrBy(ctx context.Context, pk, member string, delta float64) (float64, bool, error) {
	newScore, isNew, err := t.inner.ZIncrBy(ctx, pk, member, delta)
	return newScore, isNew, t.obs(err)
}

func (t *throttleStore) ZRangeByRank(ctx context.Context, pk string, start, stop int, rev bool) ([]ZMember, error) {
	members, err := t.inner.ZRangeByRank(ctx, pk, start, stop, rev)
	return members, t.obs(err)
}

func (t *throttleStore) ZRangeByScore(ctx context.Context, pk string, min, max ScoreBound, rev bool) ([]ZMember, error) {
	members, err := t.inner.ZRangeByScore(ctx, pk, min, max, rev)
	return members, t.obs(err)
}

func (t *throttleStore) ZCount(ctx context.Context, pk string, min, max ScoreBound) (int, error) {
	count, err := t.inner.ZCount(ctx, pk, min, max)
	return count, t.obs(err)
}

func (t *throttleStore) ZRank(ctx context.Context, pk, member string, rev bool) (int, bool, error) {
	rank, found, err := t.inner.ZRank(ctx, pk, member, rev)
	return rank, found, t.obs(err)
}

func (t *throttleStore) ZRemRangeByRank(ctx context.Context, pk string, start, stop int) (int, error) {
	removed, err := t.inner.ZRemRangeByRank(ctx, pk, start, stop)
	return removed, t.obs(err)
}

func (t *throttleStore) ZRemRangeByScore(ctx context.Context, pk string, min, max ScoreBound) (int, error) {
	removed, err := t.inner.ZRemRangeByScore(ctx, pk, min, max)
	return removed, t.obs(err)
}

func (t *throttleStore) ZScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]ZMember, map[string]types.AttributeValue, error) {
	members, nextLEK, err := t.inner.ZScan(ctx, pk, lek, limit)
	return members, nextLEK, t.obs(err)
}

func (t *throttleStore) LPush(ctx context.Context, pk string, elements [][]byte) (int, error) {
	pushed, err := t.inner.LPush(ctx, pk, elements)
	return pushed, t.obs(err)
}

func (t *throttleStore) RPush(ctx context.Context, pk string, elements [][]byte) (int, error) {
	pushed, err := t.inner.RPush(ctx, pk, elements)
	return pushed, t.obs(err)
}

func (t *throttleStore) LPop(ctx context.Context, pk string) ([]byte, bool, error) {
	val, found, err := t.inner.LPop(ctx, pk)
	return val, found, t.obs(err)
}

func (t *throttleStore) RPop(ctx context.Context, pk string) ([]byte, bool, error) {
	val, found, err := t.inner.RPop(ctx, pk)
	return val, found, t.obs(err)
}

func (t *throttleStore) LRange(ctx context.Context, pk string, start, stop int) ([][]byte, error) {
	vals, err := t.inner.LRange(ctx, pk, start, stop)
	return vals, t.obs(err)
}

func (t *throttleStore) LIndex(ctx context.Context, pk string, index int) ([]byte, bool, error) {
	val, found, err := t.inner.LIndex(ctx, pk, index)
	return val, found, t.obs(err)
}

func (t *throttleStore) LRangeAll(ctx context.Context, pk string) ([][]byte, error) {
	vals, err := t.inner.LRangeAll(ctx, pk)
	return vals, t.obs(err)
}

func (t *throttleStore) LReplaceAll(ctx context.Context, pk string, elements [][]byte) (int, error) {
	count, err := t.inner.LReplaceAll(ctx, pk, elements)
	return count, t.obs(err)
}

func (t *throttleStore) ScanKeys(ctx context.Context, lek map[string]types.AttributeValue, limit int32, now int64) ([]string, map[string]types.AttributeValue, error) {
	keys, nextLEK, err := t.inner.ScanKeys(ctx, lek, limit, now)
	return keys, nextLEK, t.obs(err)
}

func (t *throttleStore) HScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]HField, map[string]types.AttributeValue, error) {
	fields, nextLEK, err := t.inner.HScan(ctx, pk, lek, limit)
	return fields, nextLEK, t.obs(err)
}

