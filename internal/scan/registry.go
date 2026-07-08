// Package scan implements the SCAN cursor registry that bridges Redis uint64
// cursors to DynamoDB LastEvaluatedKey values via an in-memory LRU.
//
// Redis clients (go-redis, jedis, redis-py) parse SCAN cursors as uint64
// integers, whereas DynamoDB paginates using an opaque LastEvaluatedKey
// structure. The Registry bridges the two inside the proxy: Save records a
// LastEvaluatedKey under a freshly generated random non-zero uint64 cursor, and
// Load exchanges a cursor back for its LastEvaluatedKey.
//
// The registry is an in-memory LRU with a 10 minute TTL and a 10k entry cap
// (see design "SCAN 游标设计"). Cursors are bound to the proxy instance that
// generated them: the Registry is per-instance and every entry records its
// owning instID, so a cursor evicted, expired, or replayed against a different
// instance yields ok=false and the caller replies
// "-ERR invalid cursor, restart scan".
package scan

import (
	"container/list"
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DefaultCapacity is the maximum number of cursors retained before the least
// recently used entry is evicted (requirement 13.2).
const DefaultCapacity = 10_000

// DefaultTTL is how long a cursor remains valid after creation (requirement
// 13.2).
const DefaultTTL = 10 * time.Minute

// CursorEntry is the value associated with a cursor: the DynamoDB pagination
// token and the time the cursor was created (used for TTL expiry).
type CursorEntry struct {
	LastEvaluatedKey map[string]types.AttributeValue
	CreatedAt        time.Time
}

// lruItem is the internal node stored in the LRU list. It carries the owning
// instance id so ownership can be validated independently of the exported
// CursorEntry shape.
type lruItem struct {
	cursor uint64
	instID string
	entry  CursorEntry
}

// Config configures a Registry. Zero-value fields fall back to production
// defaults; tests inject small capacities, short TTLs, deterministic clocks and
// cursor sources.
type Config struct {
	// InstID identifies the proxy instance that owns cursors handed out by
	// this Registry. Cursors are bound to this instance (requirement 13.6).
	InstID string
	// Capacity is the maximum number of live cursors. <=0 uses DefaultCapacity.
	Capacity int
	// TTL is the cursor lifetime. <=0 uses DefaultTTL.
	TTL time.Duration
	// Now supplies the current time; nil uses time.Now. Injectable for tests.
	Now func() time.Time
	// Rand supplies raw uint64 values for cursor generation; nil uses a
	// crypto/rand backed source. Injectable for tests.
	Rand func() uint64
}

// Registry maps uint64 cursors to DynamoDB LastEvaluatedKey values via a
// thread-safe in-memory LRU with TTL and capacity limits. It is safe for
// concurrent use by multiple connection goroutines.
type Registry struct {
	mu       sync.Mutex
	instID   string
	capacity int
	ttl      time.Duration
	now      func() time.Time
	randU64  func() uint64

	items map[uint64]*list.Element // cursor -> element holding *lruItem
	order *list.List               // front = most recently used
}

// New constructs a Registry from cfg, applying defaults for unset fields.
func New(cfg Config) *Registry {
	if cfg.Capacity <= 0 {
		cfg.Capacity = DefaultCapacity
	}
	if cfg.TTL <= 0 {
		cfg.TTL = DefaultTTL
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Rand == nil {
		cfg.Rand = cryptoRandU64
	}
	return &Registry{
		instID:   cfg.InstID,
		capacity: cfg.Capacity,
		ttl:      cfg.TTL,
		now:      cfg.Now,
		randU64:  cfg.Rand,
		items:    make(map[uint64]*list.Element),
		order:    list.New(),
	}
}

// InstID returns the identifier of the proxy instance that owns cursors handed
// out by this Registry.
func (r *Registry) InstID() string { return r.instID }

// Save records lek under a freshly generated random non-zero uint64 cursor and
// returns that cursor (requirement 13.1). Saving may evict the least recently
// used entry when the registry is at capacity (requirement 13.2).
func (r *Registry) Save(lek map[string]types.AttributeValue) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	cursor := r.newCursorLocked()
	item := &lruItem{
		cursor: cursor,
		instID: r.instID,
		entry: CursorEntry{
			LastEvaluatedKey: lek,
			CreatedAt:        r.now(),
		},
	}
	r.items[cursor] = r.order.PushFront(item)

	// Enforce the capacity cap by evicting least recently used entries.
	for r.order.Len() > r.capacity {
		r.removeElementLocked(r.order.Back())
	}
	return cursor
}

// Load returns the LastEvaluatedKey previously stored under cursor. ok is false
// when the cursor is unknown (evicted, from a restarted or different instance),
// has expired past the TTL, or is zero (requirement 13.5). Because the Registry
// is per-instance, cursors are inherently bound to the owning instance
// (requirement 13.6).
func (r *Registry) Load(cursor uint64) (map[string]types.AttributeValue, bool) {
	return r.load(cursor, r.instID)
}

// LoadOwned behaves like Load but additionally rejects cursors whose owning
// instance does not match instID. Callers pass the connection's owning instance
// id so a cursor generated by a different instance is rejected explicitly
// (requirement 13.6).
func (r *Registry) LoadOwned(cursor uint64, instID string) (map[string]types.AttributeValue, bool) {
	return r.load(cursor, instID)
}

func (r *Registry) load(cursor uint64, expectInstID string) (map[string]types.AttributeValue, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if cursor == 0 {
		return nil, false
	}
	el, ok := r.items[cursor]
	if !ok {
		return nil, false
	}
	item := el.Value.(*lruItem)

	// TTL expiry: entries older than the TTL are invalid and removed.
	if r.now().Sub(item.entry.CreatedAt) >= r.ttl {
		r.removeElementLocked(el)
		return nil, false
	}
	// Ownership: the cursor must belong to the expected instance.
	if item.instID != expectInstID {
		return nil, false
	}

	r.order.MoveToFront(el)
	return item.entry.LastEvaluatedKey, true
}

// Len returns the current number of retained cursors. Primarily for tests and
// diagnostics.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.order.Len()
}

// newCursorLocked returns a random non-zero uint64 not already in use. The
// caller must hold r.mu.
func (r *Registry) newCursorLocked() uint64 {
	for {
		c := r.randU64()
		if c == 0 {
			continue
		}
		if _, exists := r.items[c]; exists {
			continue
		}
		return c
	}
}

// removeElementLocked removes el (which may be nil) from both the order list and
// the lookup map. The caller must hold r.mu.
func (r *Registry) removeElementLocked(el *list.Element) {
	if el == nil {
		return
	}
	item := el.Value.(*lruItem)
	delete(r.items, item.cursor)
	r.order.Remove(el)
}

// cryptoRandU64 returns a cryptographically random uint64, falling back to a
// timestamp-derived value only if the system randomness source is unavailable,
// which is effectively never on supported platforms.
func cryptoRandU64() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint64(b[:])
}
