package storage

import (
	"context"
	"math"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// v1 line (redimo v1.7.2): rv1.7 adds the read-only introspection primitives
// Client.ScanKeys and Client.TypeOf that v1.6.1 lacked, so the SCAN family and TYPE
// are no longer gated. These Store methods bridge the proxy's cursor/paging contract
// onto them. Every method here performs only reads and never mutates the table.
//
//   - ScanKeys pages the WHOLE table via rv1.7's ScanKeys — a raw Scan that projects
//     only the partition key and excludes the reserved "_redimo/" metadata namespace
//     (list index counters, stream state). The DynamoDB LastEvaluatedKey is passed
//     through verbatim so the command layer's cursor registry can page it.
//   - HSCAN/SSCAN/ZSCAN have no native within-partition cursor primitive on the v1
//     backend, so they read the whole (partition-bounded) collection via rv1's
//     existing high-level reads and return it as a SINGLE TERMINAL PAGE (nextLEK nil
//     ⇒ cursor 0). Returning the entire match set in one SCAN call is a legal Redis
//     reply (COUNT is only a hint). A continuation call (non-nil lek) cannot occur —
//     no non-nil token is ever issued — but is guarded defensively to the empty tail.

func (s *redimoStore) ScanKeys(ctx context.Context, lek map[string]types.AttributeValue, limit int32, now int64) ([]string, map[string]types.AttributeValue, error) {
	// now (the expiry filter) is irrelevant on the v1 line: rv1 stores no TTL, so no
	// key is ever logically expired. ctx is accepted for signature parity (rv1 threads
	// no context).
	return s.client.ScanKeys(limit, lek)
}

func (s *redimoStore) HScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]HField, map[string]types.AttributeValue, error) {
	if len(lek) > 0 {
		return nil, nil, nil
	}
	fields, err := s.HGetAll(ctx, pk)
	return fields, nil, err
}

func (s *redimoStore) SScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]string, map[string]types.AttributeValue, error) {
	if len(lek) > 0 {
		return nil, nil, nil
	}
	members, err := s.SMembers(ctx, pk)
	return members, nil, err
}

func (s *redimoStore) ZScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]ZMember, map[string]types.AttributeValue, error) {
	if len(lek) > 0 {
		return nil, nil, nil
	}
	// Read every member/score via an unbounded ZRANGEBYSCORE over the full score
	// range (count 0 = no limit), the same whole-collection read the v1 store uses
	// elsewhere. Order is unspecified, matching Redis ZSCAN.
	scores, err := s.client.ZRANGEBYSCORE(pk, math.Inf(-1), math.Inf(1), 0, 0)
	if err != nil {
		return nil, nil, err
	}
	members := make([]ZMember, 0, len(scores))
	for member, score := range scores {
		members = append(members, ZMember{Member: member, Score: score})
	}
	return members, nil, nil
}
