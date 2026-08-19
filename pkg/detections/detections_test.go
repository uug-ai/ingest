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
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	projectId := primitive.NewObjectID()
	run := models.DetectionRun{
		Key:            "media-1",
		OrganisationId: "org-1",
		ProjectId:      &projectId,
	}
	run.Source.RunId = "run-1"

	// Seed a run written before the project axis existed. The tolerant predicate
	// must refresh and back-fill it rather than insert a second document beside
	// it — this is the case a strict projectId clause would silently duplicate.
	if _, err := db.Collection(DetectionsCollection).InsertOne(ctx, bson.M{
		"key": run.Key, "organisationId": run.OrganisationId,
		"source": bson.M{"runId": run.Source.RunId},
	}); err != nil {
		t.Fatalf("seed legacy run: %v", err)
	}

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

	var stored bson.M
	if err := db.Collection(DetectionsCollection).FindOne(ctx, bson.M{"source.runId": run.Source.RunId}).Decode(&stored); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if stored["projectId"] != projectId {
		t.Errorf("stored projectId = %v, want %v back-filled onto the pre-rollout run", stored["projectId"], projectId)
	}
}

// TestUpsertFilterScopesToProjectTolerantly pins the identity filter's tenant
// axes without a database. The project arm must keep matching runs written
// before the field existed: a strict clause would fail to find the writer's own
// earlier document and insert a duplicate instead of refreshing it.
func TestUpsertFilterScopesToProjectTolerantly(t *testing.T) {
	projectId := primitive.NewObjectID()
	run := models.DetectionRun{Key: "media-1", OrganisationId: "org-1", ProjectId: &projectId}
	run.Source.RunId = "run-1"

	filter := upsertFilter(run)

	if filter["key"] != "media-1" || filter["organisationId"] != "org-1" || filter["source.runId"] != "run-1" {
		t.Errorf("filter = %#v, want the (key, organisationId, source.runId) identity intact", filter)
	}
	predicate, ok := filter["projectId"].(bson.M)
	if !ok {
		t.Fatalf("projectId = %#v, want a bson.M predicate", filter["projectId"])
	}
	values, ok := predicate["$in"].(bson.A)
	if !ok || len(values) != 2 || values[0] != projectId || values[1] != nil {
		t.Errorf("projectId predicate = %#v, want an $in of [%v <nil>]", predicate, projectId)
	}

	t.Run("an unprojected run stays organisation-wide", func(t *testing.T) {
		bare := upsertFilter(models.DetectionRun{Key: "media-1", OrganisationId: "org-1"})
		if _, scoped := bare["projectId"]; scoped {
			t.Error("a run with no project must not gain a projectId clause")
		}
	})
}
