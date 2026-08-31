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
	organisation := primitive.NewObjectID()
	// The organisation's default project: the only project whose clause is
	// tolerant of unstamped runs, which is what the seeded legacy run below needs.
	projectId := models.DefaultProjectId(organisation)
	run := models.DetectionRun{
		Key:            "media-1",
		OrganisationId: organisation.Hex(),
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

// TestUpsertFilterScopesToProject pins the identity filter's tenant axes without
// a database.
//
// The default-project arm must keep matching runs written before the field
// existed: a strict clause there would fail to find the writer's own earlier
// document and insert a duplicate instead of refreshing it. A NON-default
// project must be strict — an unconditionally tolerant clause reads the same
// today but would let a second project's upsert adopt an unstamped run that
// belongs to the default project.
func TestUpsertFilterScopesToProject(t *testing.T) {
	organisation := primitive.NewObjectID()
	defaultProject := models.DefaultProjectId(organisation)
	run := models.DetectionRun{Key: "media-1", OrganisationId: organisation.Hex(), ProjectId: &defaultProject}
	run.Source.RunId = "run-1"

	filter := upsertFilter(run)

	if filter["key"] != "media-1" || filter["organisationId"] != organisation.Hex() || filter["source.runId"] != "run-1" {
		t.Errorf("filter = %#v, want the (key, organisationId, source.runId) identity intact", filter)
	}
	if _, merged := filter["projectId"]; merged {
		t.Fatalf("filter = %#v, want the project clause under $and, not merged as a top-level key", filter)
	}
	and, ok := filter["$and"].([]bson.M)
	if !ok || len(and) != 1 {
		t.Fatalf("$and = %#v, want exactly the project clause", filter["$and"])
	}
	condition, ok := and[0]["projectId"].(bson.M)
	if !ok {
		t.Fatalf("project clause = %#v, want a projectId condition", and[0])
	}
	values, ok := condition["$in"].(bson.A)
	if !ok || len(values) != 2 || values[0] != defaultProject || values[1] != nil {
		t.Errorf("projectId condition = %#v, want $in [%v, nil]", condition, defaultProject)
	}

	t.Run("a real project matches only its own runs", func(t *testing.T) {
		realProject := primitive.NewObjectID()
		run := run
		run.ProjectId = &realProject

		and, ok := upsertFilter(run)["$and"].([]bson.M)
		if !ok || len(and) != 1 {
			t.Fatalf("$and = %#v, want exactly the project clause", and)
		}
		if and[0]["projectId"] != realProject {
			t.Errorf("project clause = %#v, want strict equality on %v", and[0], realProject)
		}
		if _, tolerant := and[0]["$or"]; tolerant {
			t.Error("a non-default project must not match unstamped runs: it would adopt " +
				"a run that belongs to the organisation's default project")
		}
	})

	t.Run("an unprojected run stays organisation-wide", func(t *testing.T) {
		bare := upsertFilter(models.DetectionRun{Key: "media-1", OrganisationId: organisation.Hex()})
		for _, key := range []string{"projectId", "$and"} {
			if _, scoped := bare[key]; scoped {
				t.Errorf("a run with no project must not gain a %q clause", key)
			}
		}
	})
}

// TestUpsertSetCarriesProject pins the other half of the upsert. For the
// organisation's default project the identity filter's project clause is an
// $or, and Mongo seeds an upserted document only from a filter's *equality*
// clauses — so the filter alone would insert a run with no projectId at all.
// The $set is the run itself, and models.DetectionRun tags ProjectId
// `bson:"projectId,omitempty"`, so a resolved project is what stamps a fresh
// insert and back-fills a matched legacy run.
func TestUpsertSetCarriesProject(t *testing.T) {
	organisation := primitive.NewObjectID()
	projectId := models.DefaultProjectId(organisation)
	run := models.DetectionRun{Key: "media-1", OrganisationId: organisation.Hex(), ProjectId: &projectId}

	raw, err := bson.Marshal(run)
	if err != nil {
		t.Fatalf("marshal run: %v", err)
	}
	var doc bson.M
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal run: %v", err)
	}
	if got := doc["projectId"]; got != projectId {
		t.Errorf("$set projectId = %v, want %v — without it an upsert under the "+
			"tolerant $in clause inserts an unstamped run", got, projectId)
	}
}
