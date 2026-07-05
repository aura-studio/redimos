package storage

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (s *redimoStore) ScanKeys(ctx context.Context, lek map[string]types.AttributeValue, limit int32, now int64) ([]string, map[string]types.AttributeValue, error) {
	// Thread ctx into the DynamoDB Scan so a SCAN-command deadline/cancellation
	// (see the --scan-timeout guard in the command layer) actually aborts the
	// backend call instead of being ignored.
	return s.client.WithContext(ctx).ScanMetaKeys(limit, lek, now)
}

func (s *redimoStore) HScan(_ context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]HField, map[string]types.AttributeValue, error) {
	// ctx is accepted by the seam but not yet threaded down: redimo v1.7 uses
	// context.TODO() internally.
	//
	// HScanPage Queries WITHIN the single partition and returns one page of field
	// items, already excluding the reserved meta item (sk == MetaSK). Each field's
	// value is read back as opaque bytes exactly as HGet/HGetAll do, so HSCAN
	// round-trips arbitrary field values. The MATCH filter on the field name is
	// applied proxy-side by the command layer, and the nextLEK token is bridged to
	// a uint64 cursor by the SCAN registry the HSCAN handler shares with SCAN.
	page, nextLEK, err := s.client.HScanPage(pk, limit, lek)
	if err != nil {
		return nil, nil, err
	}

	fields := make([]HField, 0, len(page))
	for _, f := range page {
		fields = append(fields, HField{Field: f.Field, Value: f.Value.Bytes()})
	}

	return fields, nextLEK, nil
}

func (s *redimoStore) SScan(_ context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]string, map[string]types.AttributeValue, error) {
	// ctx is accepted by the seam but not yet threaded down: redimo v1.7 uses
	// context.TODO() internally.
	//
	// A Set stores each member as a sort-key item under the pk exactly like a Hash
	// stores each field, so SSCAN reuses the fork's single-partition page primitive
	// (HScanPage) and keeps only the member NAME (the item's sort key) — a set
	// member carries no value attribute. HScanPage already excludes the reserved
	// meta item (sk == MetaSK), so it is never surfaced as a member. The MATCH
	// filter on the member name is applied proxy-side by the command layer, and the
	// nextLEK token is bridged to a uint64 cursor by the SCAN registry the SSCAN
	// handler shares with SCAN.
	page, nextLEK, err := s.client.HScanPage(pk, limit, lek)
	if err != nil {
		return nil, nil, err
	}

	members := make([]string, 0, len(page))
	for _, m := range page {
		members = append(members, m.Field)
	}

	return members, nextLEK, nil
}

func (s *redimoStore) ZScan(_ context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]ZMember, map[string]types.AttributeValue, error) {
	// ctx is accepted by the seam but not yet threaded down: redimo v1.7 uses
	// context.TODO() internally.
	//
	// A Sorted Set stores each member as a sort-key item under the pk with its
	// score in the numeric sort-key attribute (skN), so ZSCAN reuses the fork's
	// single-partition page primitive dedicated to sorted sets (ZScanPage), which
	// — unlike HScanPage — decodes each item to a member (the sort key) AND its
	// score (skN). ZScanPage already excludes the reserved meta item (sk ==
	// MetaSK), so it is never surfaced as a member. The page is iterated in
	// base-table (member) order, not score order — ZSCAN makes no ordering
	// guarantee. The MATCH filter on the member name is applied proxy-side by the
	// command layer, and the nextLEK token is bridged to a uint64 cursor by the
	// SCAN registry the ZSCAN handler shares with SCAN.
	page, nextLEK, err := s.client.ZScanPage(pk, limit, lek)
	if err != nil {
		return nil, nil, err
	}

	members := make([]ZMember, 0, len(page))
	for _, m := range page {
		members = append(members, ZMember{Member: m.Member, Score: m.Score})
	}

	return members, nextLEK, nil
}
