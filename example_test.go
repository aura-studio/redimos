package redimos_test

import (
	"context"
	"fmt"
	"log"

	"github.com/aura-studio/redimos/v2"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Example_inProcessClient shows how to embed redimos in-process and drive it with the
// standard go-redis client over an in-memory connection — no TCP, no kernel
// networking. Deletes are synchronous and no background goroutines are started.
//
// It has no "// Output:" comment, so `go test` compiles and type-checks it (keeping it
// in sync with the real API) but does NOT execute it — the suite stays green offline
// without a live DynamoDB.
func Example_inProcessClient() {
	// A DynamoDB client. Here it points at a local dynamodb-local with dummy
	// credentials; in production use the default AWS credential/region chain instead.
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "dummy", SecretAccessKey: "dummy", Source: "example"}, nil
		})),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000", PartitionID: "aws", SigningRegion: "us-east-1"}, nil
			})),
	)
	if err != nil {
		log.Fatal(err)
	}
	ddb := dynamodb.NewFromConfig(cfg)

	// Build the in-process client. The returned *redis.Client is an ordinary go-redis
	// client, but it talks to redimos in-process over an in-memory conn.
	client, closer, err := redimos.NewClient(ddb, redimos.Options{
		Table:   "redis-data",
		MultiDB: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer closer.Close()

	ctx := context.Background()

	// Standard go-redis usage — SET / GET.
	if err := client.Set(ctx, "greeting", "hello", 0).Err(); err != nil {
		log.Fatal(err)
	}
	val, err := client.Get(ctx, "greeting").Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(val)

	// Hashes. redimos targets Redis 3.2, whose HSET sets a single field/value per
	// call (multi-field HSET is 4.0+); use one call per field, or HMSet for many.
	if err := client.HSet(ctx, "user:1", "name", "ada").Err(); err != nil {
		log.Fatal(err)
	}
	name, _ := client.HGet(ctx, "user:1", "name").Result()
	fmt.Println(name)

	// DEL is synchronous here: the key's members are reclaimed before Del returns.
	if err := client.Del(ctx, "greeting", "user:1").Err(); err != nil {
		log.Fatal(err)
	}
}

// Example_inProcessClient_autoCreateTable shows the embedding provisioning its own
// DynamoDB table. With Options.AutoCreate set, NewClient creates the
// table with redimo's schema if it does not exist — and otherwise verifies the existing
// table is redimo-compatible — before returning, so a fresh environment needs no
// out-of-band table setup. It mirrors the cmd/redimos -auto-create-table flag and needs
// the dynamodb:DescribeTable and dynamodb:CreateTable permissions.
//
// Like the example above it has no "// Output:" line, so it is compiled and
// type-checked but not executed (no live DynamoDB needed to keep the suite green).
func Example_inProcessClient_autoCreateTable() {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "dummy", SecretAccessKey: "dummy", Source: "example"}, nil
		})),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000", PartitionID: "aws", SigningRegion: "us-east-1"}, nil
			})),
	)
	if err != nil {
		log.Fatal(err)
	}
	ddb := dynamodb.NewFromConfig(cfg)

	// AutoCreate: create the table (with redimo's schema) if it is missing, or
	// verify an existing one is compatible, before the client is returned.
	client, closer, err := redimos.NewClient(ddb, redimos.Options{
		Table:           "redis-data",
		AutoCreate: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer closer.Close()

	ctx := context.Background()
	if err := client.Set(ctx, "greeting", "hello", 0).Err(); err != nil {
		log.Fatal(err)
	}
	val, _ := client.Get(ctx, "greeting").Result()
	fmt.Println(val)
}
