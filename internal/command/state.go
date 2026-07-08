package command

import (
	"context"

	"github.com/aura-studio/redimos/internal/meta"
)

// loadMetaState is the shared meta-loading preamble for the collection command
// families (Hash/Set/List/SortedSet). It loads the key's meta item and classifies
// the key relative to the wanted type: whether it is a live key of that type,
// whether it is live but a different type (WRONGTYPE), and the loaded meta (valid
// only when the key is live and of the wanted type). An absent or expired key
// reports live=false, wrongType=false — the collection read then behaves as if the
// key were an empty collection. Expiry is judged from meta.exp against the
// router's clock, independent of DynamoDB native-TTL timing.
func (r *Router) loadMetaState(ctx context.Context, pk string, want meta.KeyType) (m meta.Meta, live, wrongType bool, err error) {
	m, found, err := r.Storage.Meta.Load(ctx, pk)
	if err != nil {
		return meta.Meta{}, false, false, err
	}
	if !found || meta.IsExpired(m, r.now()) {
		return meta.Meta{}, false, false, nil
	}

	// v1 line: redimo v1.6.1 stores no type tag, so LoadMeta returns an empty Type
	// and WRONGTYPE is UNENFORCEABLE (an accepted tradeoff — see the v1 plan). A live
	// key is therefore always treated as the wanted type: wrongType is never set, and
	// the collection read/write proceeds over whatever items live under the pk. (On
	// the v2 line this compared m.Type != want and reported WRONGTYPE.)
	m.Type = want

	return m, true, false, nil
}
