package storage

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// tableDesc builds a TableDescription fixture: pk/sk of the given key type, skN=N, the
// primary key (pk HASH, sk RANGE), and — when withLSI — an "idx" LSI whose HASH/RANGE
// are lsiHash/lsiRange.
func tableDesc(keyType types.ScalarAttributeType, withLSI bool, lsiHash, lsiRange string) *types.TableDescription {
	td := &types.TableDescription{
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: keyType},
			{AttributeName: aws.String("sk"), AttributeType: keyType},
			{AttributeName: aws.String("skN"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
	}
	if withLSI {
		td.LocalSecondaryIndexes = []types.LocalSecondaryIndexDescription{{
			IndexName: aws.String("idx"),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String(lsiHash), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String(lsiRange), KeyType: types.KeyTypeRange},
			},
		}}
	}
	return td
}

func TestCheckTableCompatible(t *testing.T) {
	// A well-formed v1 (String-key) table is accepted.
	if err := checkTableCompatible("ok", tableDesc(types.ScalarAttributeTypeS, true, "pk", "skN")); err != nil {
		t.Errorf("compatible String-key table was rejected: %v", err)
	}

	// A v2 (Binary-key) table handed to the v1 String line is rejected WITH the hint —
	// this is the footgun the check exists to catch. The fixture is named "binarytbl"
	// (not "v2tbl") so the hint assertion below cannot be satisfied tautologically by
	// the table name leaking into the %q-formatted error.
	err := checkTableCompatible("binarytbl", tableDesc(types.ScalarAttributeTypeB, true, "pk", "skN"))
	if err == nil {
		t.Fatal("Binary-key table should be rejected on the String line")
	}
	// Assert on the distinctive hint text, so a broken/missing keyTypeHint is caught.
	if !strings.Contains(err.Error(), "use the v2 proxy") {
		t.Errorf("Binary-key rejection should hint 'use the v2 proxy', got: %v", err)
	}

	// A table missing the idx LSI is rejected.
	if err := checkTableCompatible("nolsi", tableDesc(types.ScalarAttributeTypeS, false, "", "")); err == nil {
		t.Error("table without the idx LSI should be rejected")
	}

	// An idx LSI with the wrong RANGE key (sk instead of skN) is rejected.
	if err := checkTableCompatible("badlsi", tableDesc(types.ScalarAttributeTypeS, true, "pk", "sk")); err == nil {
		t.Error("idx LSI with the wrong range key should be rejected")
	}

	// A table missing the skN attribute definition is rejected.
	noSkN := tableDesc(types.ScalarAttributeTypeS, true, "pk", "skN")
	noSkN.AttributeDefinitions = noSkN.AttributeDefinitions[:2] // drop skN
	if err := checkTableCompatible("noskn", noSkN); err == nil {
		t.Error("table missing the skN attribute should be rejected")
	}
}
