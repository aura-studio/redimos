package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// healthSentinelPK is a reserved partition key that never holds real data, used to
// probe DynamoDB reachability with a GetItem. GetItem is chosen over DescribeTable so
// the probe needs only the read permission the proxy already has (a DescribeTable
// grant would otherwise be required), while still detecting an unreachable endpoint,
// a missing table (ResourceNotFoundException), or denied credentials.
var healthSentinelPK = []byte("\x00__redimos_health_probe__")

// checkBackend issues one bounded GetItem on the sentinel key. A missing item is the
// expected healthy result (nil error); any error (network, ResourceNotFound, access
// denied, throttle) means the backend is not usable right now.
func checkBackend(ctx context.Context, ddb *dynamodb.Client, table string, timeout time.Duration) error {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	_, err := ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberB{Value: healthSentinelPK},
			"sk": &types.AttributeValueMemberB{Value: []byte{0}},
		},
	})
	return err
}

// backendProbe periodically runs checkBackend and caches the outcome so /readyz can
// reflect backend health with a non-blocking atomic read — never a per-request
// DynamoDB call. It is a Start/Stop background worker like the deleter/sweeper.
type backendProbe struct {
	ddb      *dynamodb.Client
	table    string
	interval time.Duration
	timeout  time.Duration

	healthy atomic.Bool
	mu      sync.Mutex
	lastErr string

	started   atomic.Bool
	startOnce sync.Once
	stopOnce  sync.Once
	quit      chan struct{}
	done      chan struct{}
}

// newBackendProbe builds a probe. seedHealthy sets the initial cached state (the
// caller passes true after a successful startup validation so /readyz is ready
// immediately, without waiting for the first tick).
func newBackendProbe(ddb *dynamodb.Client, table string, interval, timeout time.Duration, seedHealthy bool) *backendProbe {
	p := &backendProbe{
		ddb:      ddb,
		table:    table,
		interval: interval,
		timeout:  timeout,
		quit:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	p.healthy.Store(seedHealthy)
	return p
}

// Healthy reports the last cached probe outcome.
func (p *backendProbe) Healthy() bool { return p.healthy.Load() }

// LastError returns the most recent probe error string, or "" when healthy.
func (p *backendProbe) LastError() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastErr
}

// Start launches the background prober. Idempotent.
func (p *backendProbe) Start(ctx context.Context) {
	p.startOnce.Do(func() {
		p.started.Store(true)
		go p.run(ctx)
	})
}

// Stop halts the prober and waits for it to exit. Idempotent and safe if never
// started (returns immediately).
func (p *backendProbe) Stop() {
	if !p.started.Load() {
		return
	}
	p.stopOnce.Do(func() { close(p.quit) })
	<-p.done
}

func (p *backendProbe) run(ctx context.Context) {
	defer close(p.done)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.quit:
			return
		case <-ticker.C:
			p.probe(ctx)
		}
	}
}

func (p *backendProbe) probe(ctx context.Context) {
	err := checkBackend(ctx, p.ddb, p.table, p.timeout)
	p.healthy.Store(err == nil)
	p.mu.Lock()
	if err != nil {
		p.lastErr = err.Error()
	} else {
		p.lastErr = ""
	}
	p.mu.Unlock()
}
