package detections

import (
	"context"
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

// Store must satisfy the ingest detection sink so the orchestrator can inject it
// without importing this package. This assertion lives in a test file so the
// production detections package stays free of any dependency on the orchestrator.
var _ ingest.DetectionStore = (*Store)(nil)

// TestUpsertDetectionRunIsIdempotent verifies the writer replaces rather than
// duplicates a run with the same (key, organisationId, source.runId) against a
// real Mongo. It is skipped unless DETECTIONS_TEST_MONGO_URI is set, and routes
// every write to a throwaway database it drops afterwards.
func TestUpsertDetectionRunIsIdempotent(t *testing.T) {
	uri := os.Getenv("DETECTIONS_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("set DETECTIONS_TEST_MONGO_URI to run the detections store integration test")
	}

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	// Route writes to a throwaway database and drop it afterwards so the test is
	// self-contained and side-effect free.
	dbName := fmt.Sprintf("detections_test_%d", time.Now().UnixNano())
	db := client.Database(dbName)
	defer func() { _ = db.Drop(context.Background()) }()

	store := NewStore(db)
	run := models.DetectionRun{
		Key:            "media-1",
		OrganisationId: "org-1",
	}
	run.Source.RunId = "run-1"

	// Upsert the same run twice: identity keying collapses the two writes into a
	// single detection document.
	for i := 0; i < 2; i++ {
		if err := store.UpsertDetectionRun(ctx, run); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}

	n, err := db.Collection(DetectionsCollection).CountDocuments(ctx, bson.M{"source.runId": run.Source.RunId})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("detection documents after two upserts = %d, want 1", n)
	}
}
