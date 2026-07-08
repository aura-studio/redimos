package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	redimo "github.com/aura-studio/redimo"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// The redimo single-table schema this line requires. redimos builds its redimo client
// with redimo's DEFAULT attribute names (see redimo.NewClient / storage.New): partition
// key "pk", sort key "sk", numeric sort key "skN", and a local secondary index "idx" on
// (pk, skN). The v1 line stores String(S) keys; the v2 line stores Binary(B) keys —
// expectedKeyType is the single per-line difference the compatibility check pins, and is
// what catches the classic footgun of pointing one line at the other line's table.
const (
	attrPK  = "pk"
	attrSK  = "sk"
	attrSKN = "skN"
	lsiName = "idx"

	expectedKeyType = types.ScalarAttributeTypeS // v1 line: String keys
)

// EnsureTable makes the DynamoDB table ready before serving; it backs the
// -auto-create-table flag and is called only when that flag is set. If the table is
// missing it is created on-demand with redimo's exact schema (pk/sk as HASH+RANGE, the
// skN "idx" LSI), which is compatible by construction. If it already exists its schema
// is verified — key attribute types, primary key schema and the idx LSI — and an
// incompatibility (most usefully a v2 Binary-key table handed to the v1 String line, or
// vice-versa) is reported as a fatal startup error instead of surfacing later as a
// cryptic per-command failure. Requires dynamodb:DescribeTable and, to create,
// dynamodb:CreateTable.
func EnsureTable(ctx context.Context, ddb *dynamodb.Client, tableName string) error {
	out, err := ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)})
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if !errors.As(err, &notFound) {
			return fmt.Errorf("describe table %q: %w", tableName, err)
		}
		// Missing → create with redimo's schema (on-demand billing, no capacity planning).
		if cerr := redimo.NewClient(ddb).Table(tableName).CreatePayPerRequestTable(); cerr != nil {
			return fmt.Errorf("create table %q: %w", tableName, cerr)
		}
		return waitTableActive(ctx, ddb, tableName, 2*time.Minute)
	}
	return checkTableCompatible(tableName, out.Table)
}

// waitTableActive polls DescribeTable until the table reports ACTIVE (real DynamoDB
// creates asynchronously; DynamoDB Local is effectively instant), bounded by timeout.
func waitTableActive(ctx context.Context, ddb *dynamodb.Client, tableName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		out, err := ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)})
		if err == nil && out.Table != nil && out.Table.TableStatus == types.TableStatusActive {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("table %q did not become ACTIVE within %s", tableName, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// checkTableCompatible verifies an existing table matches redimo's single-table layout.
// It is pure (no I/O) so it is unit-testable without DynamoDB.
func checkTableCompatible(name string, t *types.TableDescription) error {
	if t == nil {
		return fmt.Errorf("table %q description is empty", name)
	}

	// 1) Key attribute types: pk/sk are the line's key type, skN is Number.
	attrType := map[string]types.ScalarAttributeType{}
	for _, ad := range t.AttributeDefinitions {
		if ad.AttributeName != nil {
			attrType[*ad.AttributeName] = ad.AttributeType
		}
	}
	for _, want := range []struct {
		attr string
		typ  types.ScalarAttributeType
	}{
		{attrPK, expectedKeyType},
		{attrSK, expectedKeyType},
		{attrSKN, types.ScalarAttributeTypeN},
	} {
		got, ok := attrType[want.attr]
		if !ok {
			return fmt.Errorf("table %q is not redimo-compatible: missing key attribute %q (need type %s)", name, want.attr, want.typ)
		}
		if got != want.typ {
			return fmt.Errorf("table %q is not redimo-compatible: attribute %q is type %s, this line requires %s%s",
				name, want.attr, got, want.typ, keyTypeHint(want.attr, got, want.typ))
		}
	}

	// 2) Primary key schema: pk HASH, sk RANGE.
	if err := checkKeySchema(name, "primary key", t.KeySchema, attrPK, attrSK); err != nil {
		return err
	}

	// 3) The idx local secondary index: pk HASH, skN RANGE.
	var lsi *types.LocalSecondaryIndexDescription
	for i := range t.LocalSecondaryIndexes {
		if idx := t.LocalSecondaryIndexes[i]; idx.IndexName != nil && *idx.IndexName == lsiName {
			lsi = &t.LocalSecondaryIndexes[i]
			break
		}
	}
	if lsi == nil {
		return fmt.Errorf("table %q is not redimo-compatible: missing local secondary index %q on (%s, %s)", name, lsiName, attrPK, attrSKN)
	}
	return checkKeySchema(name, "index "+lsiName, lsi.KeySchema, attrPK, attrSKN)
}

// checkKeySchema verifies a key schema is exactly (HASH=wantHash, RANGE=wantRange).
func checkKeySchema(table, which string, schema []types.KeySchemaElement, wantHash, wantRange string) error {
	var hash, rnge string
	for _, k := range schema {
		if k.AttributeName == nil {
			continue
		}
		switch k.KeyType {
		case types.KeyTypeHash:
			hash = *k.AttributeName
		case types.KeyTypeRange:
			rnge = *k.AttributeName
		}
	}
	if hash != wantHash || rnge != wantRange {
		return fmt.Errorf("table %q is not redimo-compatible: %s key schema is (HASH=%q, RANGE=%q), need (HASH=%q, RANGE=%q)",
			table, which, hash, rnge, wantHash, wantRange)
	}
	return nil
}

// keyTypeHint adds the common-mistake note when a pk/sk key type is the OTHER line's
// type (String vs Binary), which almost always means the wrong table was configured.
func keyTypeHint(attr string, got, want types.ScalarAttributeType) string {
	if attr == attrSKN {
		return ""
	}
	switch {
	case got == types.ScalarAttributeTypeB && want == types.ScalarAttributeTypeS:
		return " — this looks like a v2 (Binary-key) table; use the v2 proxy for it"
	case got == types.ScalarAttributeTypeS && want == types.ScalarAttributeTypeB:
		return " — this looks like a v1 (String-key) table; use the v1 proxy for it"
	default:
		return ""
	}
}
