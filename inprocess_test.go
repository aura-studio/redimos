package redimos_test

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/aura-studio/redimos/v2"
	"github.com/aura-studio/redimos/v2/internal/storage"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/redis/go-redis/v9"
)

// The in-process embedding tests drive the returned go-redis client end-to-end
// against DynamoDB Local. They are gated on an endpoint so a bare offline
// `go test ./...` skips cleanly:
//
//	REDIMOS_DDB_ENDPOINT  DynamoDB Local endpoint (e.g. http://host.docker.internal:8000)
//	REDIMOS_DDB_TABLE     table name (v1: redis-data-v1 [S keys]; v2: redis-data [B keys])
//
// Under the docker harness both are set. Without them the suite is skipped, keeping
// the regression gate green with no network.
func ddbEndpoint(t *testing.T) (endpoint, table string) {
	t.Helper()
	endpoint = os.Getenv("REDIMOS_DDB_ENDPOINT")
	table = os.Getenv("REDIMOS_DDB_TABLE")
	if endpoint == "" || table == "" {
		t.Skip("REDIMOS_DDB_ENDPOINT / REDIMOS_DDB_TABLE not set; skipping in-process embedding test")
	}
	return endpoint, table
}

// newDDB builds a *dynamodb.Client pointed at the local endpoint with dummy static
// credentials, mirroring the -endpoint-url convenience the TCP binary offers.
func newDDB(t *testing.T, endpoint string) *dynamodb.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "dummy", SecretAccessKey: "dummy", Source: "test"}, nil
		})),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint, PartitionID: "aws", SigningRegion: "us-east-1"}, nil
			})),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return dynamodb.NewFromConfig(cfg)
}

// TestInProcessClient_Commands drives the standard go-redis client over the in-memory
// connection through the core command families and asserts the replies, exercising
// the full transport (buffered in-mem conn + ServeConn) end to end.
func TestInProcessClient_Commands(t *testing.T) {
	endpoint, table := ddbEndpoint(t)
	ddb := newDDB(t, endpoint)

	client, closer, err := redimos.NewInProcessClient(ddb, redimos.Options{Table: table})
	if err != nil {
		t.Fatalf("NewInProcessClient: %v", err)
	}
	defer closer.Close()

	ctx := context.Background()
	kp := "inproc:cmds:" + time.Now().Format("150405.000")

	// Clean slate.
	client.Del(ctx, kp+":str", kp+":hash", kp+":list", kp+":set")

	// SET / GET.
	if err := client.Set(ctx, kp+":str", "hello", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := client.Get(ctx, kp+":str").Result(); err != nil || got != "hello" {
		t.Fatalf("GET = %q, %v; want \"hello\", nil", got, err)
	}

	// HSET / HGET / HGETALL. redimos (Redis 3.2) HSET is single field/value per call
	// (multi-field HSET is Redis 4.0+), so set the two fields with two calls.
	if err := client.HSet(ctx, kp+":hash", "f1", "v1").Err(); err != nil {
		t.Fatalf("HSET f1: %v", err)
	}
	if err := client.HSet(ctx, kp+":hash", "f2", "v2").Err(); err != nil {
		t.Fatalf("HSET f2: %v", err)
	}
	if got, err := client.HGet(ctx, kp+":hash", "f1").Result(); err != nil || got != "v1" {
		t.Fatalf("HGET = %q, %v; want \"v1\", nil", got, err)
	}
	all, err := client.HGetAll(ctx, kp+":hash").Result()
	if err != nil || all["f1"] != "v1" || all["f2"] != "v2" || len(all) != 2 {
		t.Fatalf("HGETALL = %v, %v; want {f1:v1,f2:v2}", all, err)
	}

	// LPUSH / LRANGE.
	if err := client.LPush(ctx, kp+":list", "c", "b", "a").Err(); err != nil {
		t.Fatalf("LPUSH: %v", err)
	}
	lr, err := client.LRange(ctx, kp+":list", 0, -1).Result()
	if err != nil || len(lr) != 3 || lr[0] != "a" || lr[2] != "c" {
		t.Fatalf("LRANGE = %v, %v; want [a b c]", lr, err)
	}

	// SADD / SMEMBERS.
	if err := client.SAdd(ctx, kp+":set", "x", "y", "z").Err(); err != nil {
		t.Fatalf("SADD: %v", err)
	}
	sm, err := client.SMembers(ctx, kp+":set").Result()
	if err != nil || len(sm) != 3 {
		t.Fatalf("SMEMBERS = %v, %v; want 3 members", sm, err)
	}

	// DEL.
	if n, err := client.Del(ctx, kp+":str", kp+":hash", kp+":list", kp+":set").Result(); err != nil || n != 4 {
		t.Fatalf("DEL = %d, %v; want 4", n, err)
	}
	if n, err := client.Exists(ctx, kp+":str", kp+":hash", kp+":list", kp+":set").Result(); err != nil || n != 0 {
		t.Fatalf("EXISTS after DEL = %d, %v; want 0", n, err)
	}
}

// TestInProcessClient_SynchronousDelete proves DEL reclaims members SYNCHRONOUSLY:
// immediately after DEL returns (no wait, no background worker), a direct Store
// DeleteMembers on the same partition finds ZERO members left. If DEL were the async
// lazy-delete path and the worker had not yet run, the direct reclaim would report
// >0 members still physically present under the pk.
func TestInProcessClient_SynchronousDelete(t *testing.T) {
	endpoint, table := ddbEndpoint(t)
	ddb := newDDB(t, endpoint)

	client, closer, err := redimos.NewInProcessClient(ddb, redimos.Options{Table: table})
	if err != nil {
		t.Fatalf("NewInProcessClient: %v", err)
	}
	defer closer.Close()

	ctx := context.Background()
	key := "inproc:syncdel:" + time.Now().Format("150405.000")

	// A set with several members so there is real member data to reclaim.
	if err := client.SAdd(ctx, key, "m1", "m2", "m3", "m4", "m5").Err(); err != nil {
		t.Fatalf("SADD: %v", err)
	}
	if c, err := client.SCard(ctx, key).Result(); err != nil || c != 5 {
		t.Fatalf("SCARD = %d, %v; want 5", c, err)
	}

	// DEL — must be synchronous in the embedding.
	if n, err := client.Del(ctx, key).Result(); err != nil || n != 1 {
		t.Fatalf("DEL = %d, %v; want 1", n, err)
	}

	// The key is logically gone via the client (meta removed).
	if c, err := client.SCard(ctx, key).Result(); err != nil || c != 0 {
		t.Fatalf("SCARD after DEL = %d, %v; want 0", c, err)
	}

	// Strongest assertion: a fresh, independent Store over the SAME table finds NO
	// members to reclaim under this pk — proving DEL physically removed them before it
	// returned (single-DB: pk == raw key). Under the async path (worker not yet run)
	// this would return >0.
	store := storage.New(ddb, storage.Options{TableName: table})
	if n, err := store.DeleteMembers(ctx, key); err != nil {
		t.Fatalf("direct DeleteMembers: %v", err)
	} else if n != 0 {
		t.Fatalf("after synchronous DEL, %d members still present under pk %q; want 0 (DEL was not synchronous)", n, key)
	}
}

// TestInProcessClient_NoGoroutineLeak verifies the embedding starts no background
// goroutines beyond the per-connection serving goroutines, and that Close ends those:
// NumGoroutine returns to (approximately) its pre-construction baseline after Close.
func TestInProcessClient_NoGoroutineLeak(t *testing.T) {
	endpoint, table := ddbEndpoint(t)
	ddb := newDDB(t, endpoint)

	runtime.GC()
	base := runtime.NumGoroutine()

	client, closer, err := redimos.NewInProcessClient(ddb, redimos.Options{Table: table})
	if err != nil {
		t.Fatalf("NewInProcessClient: %v", err)
	}

	ctx := context.Background()
	key := "inproc:leak:" + time.Now().Format("150405.000")
	// Drive a few commands so the client actually dials (spawning ServeConn goroutines).
	for i := 0; i < 3; i++ {
		if err := client.Set(ctx, key, "v", 0).Err(); err != nil {
			t.Fatalf("SET: %v", err)
		}
	}
	client.Del(ctx, key)

	// Close the client (returns pooled conns) then the embedding (closes server-side
	// conns, ending every ServeConn goroutine at EOF).
	if err := client.Close(); err != nil {
		t.Fatalf("client.Close: %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("closer.Close: %v", err)
	}

	// Allow the serving goroutines to observe EOF and unwind.
	deadline := time.Now().Add(5 * time.Second)
	var now int
	for time.Now().Before(deadline) {
		runtime.GC()
		now = runtime.NumGoroutine()
		if now <= base+2 { // small slack for runtime/GC bookkeeping goroutines
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutine leak: baseline %d, after Close %d (want <= %d)", base, now, base+2)
}

// TestInProcessClient_Pipelining proves the buffered in-memory conn supports go-redis
// PIPELINING (not just the connection handshake): several commands are batched and
// flushed together over the buffered conn, and must not deadlock (the property
// net.Pipe fails) and must return correct, ordered replies.
//
// It also probes client.TxPipeline() (MULTI/EXEC) and asserts it FAILS — redimos gates
// transactions — documenting that plain pipelining works while transactional
// pipelining does not.
func TestInProcessClient_Pipelining(t *testing.T) {
	endpoint, table := ddbEndpoint(t)
	ddb := newDDB(t, endpoint)

	client, closer, err := redimos.NewInProcessClient(ddb, redimos.Options{Table: table})
	if err != nil {
		t.Fatalf("NewInProcessClient: %v", err)
	}
	defer closer.Close()

	ctx := context.Background()
	base := "inproc:pipe:" + time.Now().Format("150405.000")
	kStr := base + ":str"
	kList := base + ":list"
	client.Del(ctx, kStr, kList)

	// --- non-transactional pipeline: batch several commands, flush once ----------
	var (
		setCmd    *redis.StatusCmd
		getCmd    *redis.StringCmd
		lpushCmd  *redis.IntCmd
		lrangeCmd *redis.StringSliceCmd
	)
	cmds, err := client.Pipelined(ctx, func(p redis.Pipeliner) error {
		setCmd = p.Set(ctx, kStr, "v", 0)
		getCmd = p.Get(ctx, kStr)
		lpushCmd = p.LPush(ctx, kList, "b", "a") // head-insert: final order [a b]
		lrangeCmd = p.LRange(ctx, kList, 0, -1)
		return nil
	})
	if err != nil {
		t.Fatalf("Pipelined returned transport error: %v", err)
	}
	if len(cmds) != 4 {
		t.Fatalf("pipeline returned %d cmds; want 4", len(cmds))
	}

	// Every queued command must have executed with the correct reply, in order.
	if err := setCmd.Err(); err != nil {
		t.Fatalf("pipelined SET: %v", err)
	}
	if got, err := getCmd.Result(); err != nil || got != "v" {
		t.Fatalf("pipelined GET = %q, %v; want \"v\", nil", got, err)
	}
	if n, err := lpushCmd.Result(); err != nil || n != 2 {
		t.Fatalf("pipelined LPUSH = %d, %v; want 2", n, err)
	}
	if got, err := lrangeCmd.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("pipelined LRANGE = %v, %v; want [a b]", got, err)
	}

	// --- transactional pipeline (MULTI/EXEC): expected to FAIL (redimos gates it) --
	kTx := base + ":tx"
	_, txErr := client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, kTx, "v", 0)
		return nil
	})
	if txErr == nil {
		t.Fatalf("TxPipelined unexpectedly succeeded; redimos gates MULTI/EXEC so it should error")
	}
	t.Logf("TxPipeline (MULTI/EXEC) correctly rejected: %v", txErr)

	client.Del(ctx, kStr, kList, kTx)
}
