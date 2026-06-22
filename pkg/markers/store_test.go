package markers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/uug-ai/ingest/pkg/ingest"
	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Store must satisfy the ingest marker sink so the orchestrator can inject it
// without importing this package. This assertion lives in a test file so the
// production markers package stays free of any dependency on the orchestrator.
var _ ingest.MarkerStore = (*Store)(nil)

// TestMarkerWriterModes verifies the one behavioural difference between the two
// entry points against a real Mongo: the upsert path is idempotent (a redelivery
// refreshes the same marker and its denormalised range), while the insert path
// appends a distinct marker each call. It is skipped unless MARKERS_TEST_MONGO_URI
// is set, and routes every write to a throwaway database so it never touches a
// real "Kerberos".
func TestMarkerWriterModes(t *testing.T) {
	uri := os.Getenv("MARKERS_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("set MARKERS_TEST_MONGO_URI to run the markers store integration test")
	}

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	// Redirect the package's hardcoded database to a throwaway, and restore +
	// drop it afterwards so the test is self-contained and side-effect free.
	dbName := fmt.Sprintf("markers_test_%d", time.Now().UnixNano())
	origDB := DatabaseName
	DatabaseName = dbName
	defer func() {
		DatabaseName = origDB
		_ = client.Database(dbName).Drop(context.Background())
	}()
	db := client.Database(dbName)

	marker := models.Marker{
		Name:           "loitering-conformance",
		OrganisationId: "org-1",
		DeviceId:       "device-1",
		StartTimestamp: 1000,
		EndTimestamp:   1010,
	}

	// Upsert the same marker twice: identity keying collapses the two writes into
	// a single marker document and a single denormalised range document.
	for i := 0; i < 2; i++ {
		if _, err := UpsertMarkerToMongodb(ctx, nil, client, marker); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}

	if got := count(t, ctx, db, MARKERS_COLLECTION, bson.M{"name": marker.Name}); got != 1 {
		t.Errorf("marker documents after two upserts = %d, want 1", got)
	}
	if got := count(t, ctx, db, MARKER_OPTION_RANGES_COLLECTION, bson.M{"value": marker.Name}); got != 1 {
		t.Errorf("range documents after two upserts = %d, want 1", got)
	}
	if got := count(t, ctx, db, MARKER_OPTIONS_COLLECTION, bson.M{"value": marker.Name}); got != 1 {
		t.Errorf("option documents after two upserts = %d, want 1", got)
	}

	// The insert path appends distinct markers, so two inserts on top of the one
	// upserted marker leave three.
	for i := 0; i < 2; i++ {
		if _, err := AddMarkerToMongodb(ctx, nil, client, marker); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if got := count(t, ctx, db, MARKERS_COLLECTION, bson.M{"name": marker.Name}); got != 3 {
		t.Errorf("marker documents after two inserts = %d, want 3", got)
	}
}

func count(t *testing.T, ctx context.Context, db *mongo.Database, coll string, filter bson.M) int64 {
	t.Helper()
	n, err := db.Collection(coll).CountDocuments(ctx, filter)
	if err != nil {
		t.Fatalf("count %s: %v", coll, err)
	}
	return n
}

// TestClassifyWriteError checks that the marker sink tags deterministic Mongo
// write rejections as permanent — so the ingest core drops rather than loops
// them — while leaving transient failures retryable. It needs no Mongo (it
// classifies synthetic driver errors) and asserts through ingest.IsPermanent,
// the exact predicate the core uses to split the retry/drop paths.
func TestClassifyWriteError(t *testing.T) {
	cases := []struct {
		name          string
		err           error
		wantPermanent bool
	}{
		{"nil", nil, false},
		{"duplicate key", mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: 11000}}}, true},
		{"validation failure (WriteException 121)", mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: 121}}}, true},
		{"validation failure (CommandError 121)", mongo.CommandError{Code: 121}, true},
		{"transient write error", mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: 91}}}, false},
		{"plain transient error", errors.New("connection reset"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyWriteError(tc.err)
			if tc.err == nil {
				if got != nil {
					t.Fatalf("classifyWriteError(nil) = %v, want nil", got)
				}
				return
			}
			if ingest.IsPermanent(got) != tc.wantPermanent {
				t.Errorf("ingest.IsPermanent(classifyWriteError(%v)) = %v, want %v", tc.err, ingest.IsPermanent(got), tc.wantPermanent)
			}
		})
	}
}
