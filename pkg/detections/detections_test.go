package detections

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

	// Seed a run written before either canonical ownership axis existed. The
	// source recording key is the stable authority that lets the default-project
	// write adopt and back-fill it rather than create a second document.
	if _, err := db.Collection(DetectionsCollection).InsertOne(ctx, bson.M{
		"key":    run.Key,
		"source": bson.M{"runId": run.Source.RunId},
	}); err != nil {
		t.Fatalf("seed legacy run: %v", err)
	}
	if _, err := db.Collection("analysis").InsertOne(ctx, bson.M{
		"key":    run.Key,
		"userid": organisation.Hex(),
	}); err != nil {
		t.Fatalf("seed legacy analysis parent: %v", err)
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
	if stored["organisationId"] != organisation.Hex() {
		t.Errorf("stored organisationId = %v, want %v back-filled from source authority", stored["organisationId"], organisation.Hex())
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

	filter := upsertFilter(run, true)

	if filter["key"] != "media-1" || filter["source.runId"] != "run-1" {
		t.Errorf("filter = %#v, want the (key, source.runId) identity intact", filter)
	}
	branches, ok := filter["$or"].(bson.A)
	if !ok || len(branches) != 2 {
		t.Fatalf("ownership branches = %#v, want canonical and canonical-missing branches", filter["$or"])
	}
	canonical := branches[0].(bson.M)
	if canonical["organisationId"] != organisation.Hex() {
		t.Fatalf("canonical branch = %#v, want exact organisation ownership", canonical)
	}
	assertProjectCondition(t, canonical, bson.M{"$in": bson.A{defaultProject, nil}})

	legacy := branches[1].(bson.M)
	organisationCondition, ok := legacy["organisationId"].(bson.M)
	if !ok {
		t.Fatalf("legacy organisation clause = %#v, want missing/empty condition", legacy["organisationId"])
	}
	if got := organisationCondition["$in"]; !bsonValuesEqual(got, bson.A{nil, ""}) {
		t.Errorf("legacy organisation condition = %#v, want only null/missing or empty", organisationCondition)
	}
	assertProjectCondition(t, legacy, bson.M{"$in": bson.A{defaultProject, nil}})

	t.Run("a real project matches only its own runs", func(t *testing.T) {
		realProject := primitive.NewObjectID()
		run := run
		run.ProjectId = &realProject

		branches := upsertFilter(run, true)["$or"].(bson.A)
		for _, branch := range branches {
			assertProjectCondition(t, branch.(bson.M), realProject)
		}
	})

	t.Run("an unprojected run requires exact canonical ownership", func(t *testing.T) {
		bare := upsertFilter(models.DetectionRun{Key: "media-1", OrganisationId: organisation.Hex()}, true)
		branches := bare["$or"].(bson.A)
		if len(branches) != 1 || branches[0].(bson.M)["organisationId"] != organisation.Hex() {
			t.Errorf("ownership branches = %#v, want exact canonical organisation only", branches)
		}
	})

	t.Run("legacy adoption requires authoritative parent proof", func(t *testing.T) {
		branches := upsertFilter(run, false)["$or"].(bson.A)
		if len(branches) != 1 || branches[0].(bson.M)["organisationId"] != organisation.Hex() {
			t.Errorf("ownership branches = %#v, want exact canonical ownership only", branches)
		}
	})
}

func TestParentMatchesRun(t *testing.T) {
	organisation := primitive.NewObjectID()
	defaultProject := models.DefaultProjectId(organisation)
	nonDefaultProject := primitive.NewObjectID()
	run := models.DetectionRun{OrganisationId: organisation.Hex(), ProjectId: &defaultProject}

	tests := []struct {
		name   string
		parent analysisParent
		run    models.DetectionRun
		want   bool
	}{
		{
			name:   "canonical default parent",
			parent: analysisParent{OrganisationId: organisation.Hex()},
			run:    run,
			want:   true,
		},
		{
			name:   "legacy userid default parent",
			parent: analysisParent{UserId: organisation.Hex()},
			run:    run,
			want:   true,
		},
		{
			name:   "legacy user_id default parent",
			parent: analysisParent{LegacyUserId: organisation.Hex()},
			run:    run,
			want:   true,
		},
		{
			name:   "canonical owner wins over matching legacy alias",
			parent: analysisParent{OrganisationId: primitive.NewObjectID().Hex(), UserId: organisation.Hex()},
			run:    run,
		},
		{
			name:   "exact non-default parent",
			parent: analysisParent{OrganisationId: organisation.Hex(), ProjectId: &nonDefaultProject},
			run:    models.DetectionRun{OrganisationId: organisation.Hex(), ProjectId: &nonDefaultProject},
			want:   true,
		},
		{
			name:   "unstamped parent excluded from non-default project",
			parent: analysisParent{OrganisationId: organisation.Hex()},
			run:    models.DetectionRun{OrganisationId: organisation.Hex(), ProjectId: &nonDefaultProject},
		},
		{
			name:   "missing run project fails closed",
			parent: analysisParent{OrganisationId: organisation.Hex()},
			run:    models.DetectionRun{OrganisationId: organisation.Hex()},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := parentMatchesRun(test.parent, test.run); got != test.want {
				t.Fatalf("parentMatchesRun() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestUniqueParentMatchesRunRequiresExactlyOneParent(t *testing.T) {
	organisation := primitive.NewObjectID()
	projectId := models.DefaultProjectId(organisation)
	run := models.DetectionRun{OrganisationId: organisation.Hex(), ProjectId: &projectId}
	matching := analysisParent{OrganisationId: organisation.Hex()}

	if uniqueParentMatchesRun(nil, run) {
		t.Fatal("zero parents allowed legacy adoption")
	}
	if !uniqueParentMatchesRun([]analysisParent{matching}, run) {
		t.Fatal("one authoritative parent did not allow legacy adoption")
	}
	if uniqueParentMatchesRun([]analysisParent{matching, matching}, run) {
		t.Fatal("ambiguous parents allowed legacy adoption")
	}
}

func assertProjectCondition(t *testing.T, branch bson.M, want any) {
	t.Helper()
	and, ok := branch["$and"].([]bson.M)
	if !ok || len(and) != 1 {
		t.Fatalf("project clauses = %#v, want exactly one", branch["$and"])
	}
	if got := and[0]["projectId"]; !bsonValuesEqual(got, want) {
		t.Errorf("project condition = %#v, want %#v", got, want)
	}
}

func bsonValuesEqual(got, want any) bool {
	gotRaw, gotErr := bson.Marshal(bson.M{"value": got})
	wantRaw, wantErr := bson.Marshal(bson.M{"value": want})
	return gotErr == nil && wantErr == nil && string(gotRaw) == string(wantRaw)
}

type fakeDetectionCollection struct {
	results []*mongo.UpdateResult
	errors  []error
	options []*options.UpdateOptions
}

func (f *fakeDetectionCollection) UpdateOne(_ context.Context, _, _ any, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	call := len(f.options)
	if len(opts) == 0 {
		f.options = append(f.options, nil)
	} else {
		f.options = append(f.options, opts[0])
	}
	return f.results[call], f.errors[call]
}

func TestUpsertDetectionRunDuplicateRetry(t *testing.T) {
	duplicateErr := mongo.WriteException{WriteErrors: mongo.WriteErrors{{Code: 11000}}}
	run := models.DetectionRun{Key: "media-1", OrganisationId: primitive.NewObjectID().Hex()}
	run.Source.RunId = "run-1"

	t.Run("matched retry succeeds", func(t *testing.T) {
		coll := &fakeDetectionCollection{
			results: []*mongo.UpdateResult{nil, {MatchedCount: 1}},
			errors:  []error{duplicateErr, nil},
		}
		if err := upsertDetectionRun(context.Background(), coll, run, false); err != nil {
			t.Fatalf("upsertDetectionRun: %v", err)
		}
		if len(coll.options) != 2 || coll.options[0] == nil || coll.options[0].Upsert == nil || !*coll.options[0].Upsert {
			t.Fatalf("first write options = %#v, want upsert", coll.options)
		}
		if coll.options[1] != nil && coll.options[1].Upsert != nil && *coll.options[1].Upsert {
			t.Fatal("duplicate retry must not upsert")
		}
	})

	t.Run("unmatched retry reports the duplicate conflict", func(t *testing.T) {
		coll := &fakeDetectionCollection{
			results: []*mongo.UpdateResult{nil, {MatchedCount: 0}},
			errors:  []error{duplicateErr, nil},
		}
		err := upsertDetectionRun(context.Background(), coll, run, false)
		if err == nil {
			t.Fatal("upsertDetectionRun returned nil after an unmatched duplicate retry")
		}
		if !mongo.IsDuplicateKeyError(err) {
			t.Errorf("error = %v, want wrapped duplicate-key conflict", err)
		}
	})

	t.Run("retry error is returned", func(t *testing.T) {
		retryErr := errors.New("connection reset")
		coll := &fakeDetectionCollection{
			results: []*mongo.UpdateResult{nil, nil},
			errors:  []error{duplicateErr, retryErr},
		}
		if err := upsertDetectionRun(context.Background(), coll, run, false); !errors.Is(err, retryErr) {
			t.Fatalf("upsertDetectionRun error = %v, want %v", err, retryErr)
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
