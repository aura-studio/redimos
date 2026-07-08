package storage

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// v1 line: redimo v1.6.1 exposes NO keyspace/partition scan primitive with a
// cursor (no ScanMetaKeys, no HScanPage/ZScanPage, no LastEvaluatedKey bridge) and
// no #meta items to page over. The SCAN family (SCAN/HSCAN/SSCAN/ZSCAN) is
// therefore GATED (unregistered) at the command layer, so these Store methods are
// never reached on a live path. They are retained only to satisfy the Store
// interface (and its throttle decorator) and return an empty, terminal page: no
// items and a nil nextLEK (the terminating cursor 0). ctx is accepted for signature
// parity but unused (rv1 has no context threading).

func (s *redimoStore) ScanKeys(ctx context.Context, lek map[string]types.AttributeValue, limit int32, now int64) ([]string, map[string]types.AttributeValue, error) {
	return []string{}, nil, nil
}

func (s *redimoStore) HScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]HField, map[string]types.AttributeValue, error) {
	return []HField{}, nil, nil
}

func (s *redimoStore) SScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]string, map[string]types.AttributeValue, error) {
	return []string{}, nil, nil
}

func (s *redimoStore) ZScan(ctx context.Context, pk string, lek map[string]types.AttributeValue, limit int32) ([]ZMember, map[string]types.AttributeValue, error) {
	return []ZMember{}, nil, nil
}
