package command

import (
	"time"

	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/metrics"
	"github.com/aura-studio/redimos/v2/internal/scan"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

// This file adds the storage-wiring seam to the command Router. It is the single
// place that assembles the meta/storage components onto a Router and registers the
// data-command families. It deliberately keeps the connection-only NewRouter(cfg)
// path (see connection.go) untouched so the handshake/connection tests continue to
// run without a storage backend.
//
// Registration seam for later tasks:
//
// A storage-backed Router is built with NewRouterWithStorage, which registers the
// connection commands and then calls registerDataCommands. Each command family
// lands in its own file with a single per-family register method
// (registerStrings, and later registerKeys/registerHashes/...); adding a family is
// a one-line addition to registerDataCommands and a new handler file — NewRouter /
// NewRouterWithStorage themselves never need to change again.

// Storage bundles the storage-layer components the data-command handlers need. It
// is assembled once at process start (task 23.1 wires it from flags) and handed to
// NewRouterWithStorage. Handlers read it through the Router.
//
// Only Store is required; Meta and Reader are derived from it when left nil so
// callers (and tests) can supply just a storage.Store. Now is the injectable clock
// used for expiry evaluation and EX/PX expiry computation; a nil Now defaults to
// the wall clock.
type Storage struct {
	// Store is the storage seam over the redimo fork. Required for a
	// storage-backed router; when nil the router registers only the connection
	// commands (matching NewRouter).
	Store storage.Store

	// Geo is the optional geospatial seam backing the GEO command family. When nil
	// the GEO commands are not registered (they fall through to the unknown-command
	// path). Production wiring supplies the same redimo-backed store here.
	Geo storage.GeoStore

	// Meta is the meta store used for type checks, existence/expiry and TTL. When
	// nil it is derived from Store with the Enqueuer below (or a no-op when that
	// is also nil).
	Meta *meta.MetaStore

	// Enqueuer is the lazy-delete seam DEL relies on: after DeleteMeta removes a
	// key's meta item (making it immediately logically absent), the pk is handed
	// to this enqueuer so the background deleter (task 11.1) reclaims the key's
	// members off the request path. It is only consulted when Meta is derived
	// from Store (i.e. Meta is nil); a caller supplying its own Meta has already
	// wired the enqueuer into it. A nil Enqueuer defaults to a no-op, so DEL still
	// removes meta correctly (orphan members are then reclaimed by the weekly
	// sweeper) and every existing constructor call keeps working unchanged.
	Enqueuer meta.DeletionEnqueuer

	// Reader drives the parallel meta+data read path (design algorithm 2). When
	// nil it is derived from Meta with the Now clock.
	Reader *meta.Reader

	// Scan is the per-instance SCAN cursor registry (internal/scan) that bridges
	// Redis' uint64 cursors to the backend's opaque pagination tokens for the SCAN
	// family (task 17.2). It is consulted only by the SCAN handler.
	//
	// InstID wiring: SCAN resolves a continuation cursor with
	// Scan.LoadOwned(cursor, conn.InstID()), which succeeds only when the cursor's
	// owning instance (the registry's InstID, stamped by Save) matches the
	// connection's InstID (set by server.Options.InstID). Production wiring
	// (task 23.1) therefore MUST construct this registry with the SAME InstID it
	// passes to server.New, e.g. derive one id and hand it to both
	// scan.New(scan.Config{InstID: id}) and server.Options{InstID: id}. When left
	// nil here a default registry with an empty InstID is created (see
	// NewRouterWithStorage); that default only paginates end-to-end when the server
	// also runs with an empty InstID, so real deployments inject a matching-InstID
	// registry. A cursor replayed against a different instance (a different
	// registry, or a mismatched InstID) is rejected with
	// "-ERR invalid cursor, restart scan" (requirement 13.6).
	Scan *scan.Registry

	// Now returns the current time in epoch seconds. It backs expiry evaluation
	// and EX/PX absolute-expiry computation. A nil Now defaults to
	// time.Now().Unix(); tests inject a fixed clock for deterministic expiry.
	Now func() int64

	// Slowlog is the slow-command ring buffer (internal/metrics) that SLOWLOG GET
	// reads read-only (requirement 18.7) and INFO summarises. It is registered on
	// the connection path (registerStubs) so SLOWLOG works even on a
	// connection-only router with no storage backend. When nil, registerStubs
	// installs a fresh default SlowLog so the observability commands always have a
	// live buffer to serve (see Router.ensureObservability).
	Slowlog *metrics.SlowLog

	// Metrics is the Prometheus collector set (internal/metrics) INFO reads to
	// surface a per-command summary (requirement 18.6). It is optional: when nil,
	// INFO still reports the mandated redis_version / role fields and omits the
	// command totals.
	Metrics *metrics.Metrics
}

// wallClock is the default epoch-seconds clock.
func wallClock() int64 { return time.Now().Unix() }

// NewRouterWithStorage builds a storage-backed Router: it registers the
// connection-management commands and every data-command family. Missing Meta /
// Reader / Now are derived from Store so callers can pass just a storage.Store.
//
// When st.Store is nil the router is equivalent to NewRouter(cfg) — only the
// connection commands are registered — so a caller can uniformly construct a
// router even before storage is available.
func NewRouterWithStorage(cfg Config, st Storage) *Router {
	if st.Now == nil {
		st.Now = wallClock
	}
	if st.Meta == nil && st.Store != nil {
		st.Meta = meta.NewMetaStore(st.Store, st.Enqueuer)
	}
	if st.Reader == nil && st.Meta != nil {
		st.Reader = meta.NewReader(st.Meta, st.Now)
	}
	if st.Scan == nil && st.Store != nil {
		// Default cursor registry. Production wiring (task 23.1) should inject a
		// registry constructed with the server's InstID so SCAN continuation
		// cursors validate; see the Storage.Scan doc comment.
		st.Scan = scan.New(scan.Config{})
	}

	r := &Router{Table: NewTable(), Config: cfg, Storage: st}
	r.registerConnection()
	r.registerDataCommands()

	return r
}

// registerDataCommands registers all storage-backed command families on the
// router's table. It is the single seam later tasks extend: each new family adds
// exactly one register call here (and its own handler file). When no storage is
// wired (Store == nil) it registers nothing, leaving a connection-only router.
func (r *Router) registerDataCommands() {
	if r.Storage.Store == nil {
		return
	}

	r.registerStrings()
	r.registerKeys()
	r.registerHashes()
	r.registerSets()
	r.registerZSets()
	r.registerLists()
	r.registerBit()

	// GEO is optional: only registered when a geospatial seam is wired, so a
	// deployment without it leaves the GEO* commands on the unknown-command path.
	if r.Storage.Geo != nil {
		r.registerGeo()
	}
}

// now returns the current epoch seconds using the router's injected clock,
// falling back to the wall clock when none is configured. Handlers use it for
// expiry evaluation and EX/PX expiry computation.
func (r *Router) now() int64 {
	if r.Storage.Now != nil {
		return r.Storage.Now()
	}

	return wallClock()
}
