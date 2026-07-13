// Package redimos exposes an in-process embedding of the redimos RESP2/Redis-3.2
// proxy over DynamoDB. NewInProcessClient returns a standard go-redis client that
// talks to the proxy over an in-memory connection — no TCP, no kernel networking —
// with synchronous deletes and zero background goroutines.
//
// It reuses the SAME command dispatch, storage seam and (recreate-guarded) member
// reclamation as the cmd/redimos TCP binary; only the transport (an in-memory conn
// instead of a TCP socket) and the delete mode (synchronous instead of the async
// lazy-delete worker) differ. The existing TCP proxy behaviour is untouched.
//
// Typical use:
//
//	client, closer, err := redimos.NewInProcessClient(ddb, redimos.Options{
//		Table:   "redis-data",
//		MultiDB: true,
//	})
//	if err != nil {
//		return err
//	}
//	defer closer.Close()
//	client.Set(ctx, "k", "v", 0) // standard *redis.Client, but in-process
package redimos

import (
	"context"
	"io"
	"net"
	"strings"

	"github.com/aura-studio/redimos/internal/command"
	"github.com/aura-studio/redimos/internal/meta"
	"github.com/aura-studio/redimos/internal/scan"
	"github.com/aura-studio/redimos/internal/server"
	"github.com/aura-studio/redimos/internal/storage"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/redis/go-redis/v9"
)

// Options configures an in-process redimos embedding. Every field is optional; the
// zero value builds a single-DB, strongly-consistent proxy over the redimo default
// table. It is the embedding-relevant subset of the cmd/redimos flags (the Store and
// Router knobs); background-worker / observability flags do not apply because the
// embedding starts none of those.
type Options struct {
	// Table is the DynamoDB single-table name (e.g. "redis-data"). Empty uses the
	// redimo default table name.
	Table string

	// Consistency selects the default read consistency: "strong" (the default; reads
	// its own writes, matching Redis) or "eventual". Any other value is treated as
	// "strong".
	Consistency string

	// MultiDB permits SELECT of a non-zero logical DB index (mapping keys to a
	// "d{n}:" pk prefix). When false, any non-zero SELECT is rejected exactly as the
	// TCP proxy rejects it.
	MultiDB bool

	// DB bounds the logical DB count SELECT accepts when MultiDB is true: a
	// valid index is [0, DB). A value <= 0 defaults to Redis 3.2's 16.
	DB int

	// MaxCollectionResult caps how many members a whole-collection reply
	// (HGETALL/SMEMBERS/LRANGE/...) may materialize before the command is rejected.
	// 0 (the default) disables the cap.
	MaxCollectionResult int

	// MaxCommandBytes rejects a single command whose raw wire size exceeds it. 0 (the
	// default) disables the check.
	MaxCommandBytes int

	// AutoCreate, when true, makes NewInProcessClient create the DynamoDB table
	// with redimo's schema if it does not exist — and otherwise verify the existing
	// table's schema is redimo-compatible — before the client is returned. It mirrors
	// the cmd/redimos -auto-create-table flag and needs dynamodb:DescribeTable and (to
	// create) dynamodb:CreateTable. Leave false (the default) to require an
	// operator-provisioned table, in which case no table-level API is called. Set Table
	// when enabling it.
	AutoCreate bool
}

// NewInProcessClient builds an in-process redimos proxy over ddb and returns a
// standard *redis.Client wired to it through an in-memory connection, an io.Closer
// that tears the proxy down, and any construction error.
//
// The returned client is an ordinary github.com/redis/go-redis/v9 client — every
// command it supports that the proxy implements works exactly as over TCP — but it
// never touches the network: each pooled connection dials a fresh buffered in-memory
// conn pair whose server end is served by server.ServeConn. Deletes are SYNCHRONOUS
// (a DEL reclaims the key's members, under the same IsLive recreate-guard, before it
// returns) and NO background goroutines are started (no async deleter worker, no
// orphan sweeper, no backend probe, no metrics HTTP server) — only the per-connection
// redcon serving goroutine spawned per dial exists, and Close ends those too.
//
// Close (the returned io.Closer) drains and closes the underlying server, which
// closes every in-memory connection and ends its serving goroutine; after Close the
// client should no longer be used (and the caller typically also closes the redis
// client, though closing the server already severs its conns).
func NewInProcessClient(ddb *dynamodb.Client, opts Options) (*redis.Client, io.Closer, error) {
	// Optional: create the table with redimo's schema if missing, or verify an existing
	// table's schema is compatible, BEFORE anything else — mirroring the CLI
	// -auto-create-table flag. Off by default, so a bare embedding touches no
	// table-level APIs (DescribeTable/CreateTable).
	if opts.AutoCreate {
		if err := storage.EnsureTable(context.Background(), ddb, opts.Table); err != nil {
			return nil, nil, err
		}
	}

	// --- storage: redimo-backed Store over the caller's DynamoDB client ---------
	store := storage.New(ddb, storage.Options{
		TableName:            opts.Table,
		EventuallyConsistent: strings.EqualFold(opts.Consistency, "eventual"),
	})

	// --- synchronous delete: reuse the deleter's process() INLINE ---------------
	//
	// The MetaStore's enqueuer is a Deleter in SYNCHRONOUS mode: DeleteMeta -> Enqueue
	// runs the SAME process(pk) (IsLive recreate-guard, then DeleteMembers, with the
	// same metrics) on the caller's goroutine, so a DEL is fully synchronous and no
	// background worker is spawned. The IsLive guard is wired exactly as the TCP
	// binary wires it, so a DEL-then-recreate never wipes the new incarnation.
	deleter := meta.NewDeleter(store, meta.DeleterConfig{
		Synchronous: true,
		Logger:      meta.StdLogger{},
		IsLive: func(ctx context.Context, pk string) (bool, error) {
			_, found, err := store.LoadMeta(ctx, pk)
			return found, err
		},
	})
	metaStore := meta.NewMetaStore(store, deleter)
	reader := meta.NewReader(metaStore, nil)

	// A per-instance SCAN cursor registry sharing the server's InstID (below) so a
	// SCAN continuation cursor validates against the connection's InstID, exactly as
	// in the TCP assembly.
	instID := newInstID()
	scanReg := scan.New(scan.Config{InstID: instID})

	// --- command router: identical construction to the TCP binary ---------------
	router := command.NewRouterWithStorage(
		command.Config{
			MultiDB:             opts.MultiDB,
			DB:                  opts.DB,
			MaxCollectionResult: opts.MaxCollectionResult,
		},
		command.Storage{
			Store:  store,
			Meta:   metaStore,
			Reader: reader,
			Scan:   scanReg,
		},
	)

	// The observed dispatcher with nil metrics/slowlog: it is a thin pass-through
	// (no metrics registry, no HTTP server) that still preserves the exact dispatch
	// contract used by the TCP path. No timeout/breaker/request-log wrappers are
	// needed for the embedding.
	dispatcher := command.NewObservedDispatcher(router, nil, nil, 0)

	// --- server shell: reused, but NEVER TCP-listened ---------------------------
	//
	// We build the same server.Server the TCP binary builds (so MaxCommandBytes,
	// per-connection state, InstID, drain/closing semantics are identical) but never
	// call ListenAndServe. Instead each go-redis dial serves one in-memory conn via
	// server.ServeConn. NONE of the background workers (deleter.Start, sweeper.Start,
	// probe.Start) are started.
	srv := server.New(server.Options{
		InstID:          instID,
		MaxCommandBytes: opts.MaxCommandBytes,
	}, dispatcher)

	c := &inProcessCloser{}

	client := redis.NewClient(&redis.Options{
		Network:         "inproc",
		Addr:            "redimos",
		Protocol:        2,    // RESP2: redimos speaks RESP2 only.
		DisableIdentity: true, // skip the CLIENT SETINFO / HELLO handshake redimos declines.
		Dialer: func(_ context.Context, _, _ string) (net.Conn, error) {
			// Each dial gets a FRESH buffered in-memory conn pair so go-redis's pooled
			// connections are independent; the server end is served by a new
			// ServeConn goroutine that lives until the conn closes.
			cl, sv := newBufConnPair()
			c.track(sv)
			go func() { _ = srv.ServeConn(sv) }()
			return cl, nil
		},
	})

	return client, c, nil
}
