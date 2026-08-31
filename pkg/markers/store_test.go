package markers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/uug-ai/ingest/pkg/ingest"
	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Store must satisfy the ingest marker sink so the orchestrator can inject it
// without importing this package. This assertion lives in a test file so the
// production markers package stays free of any dependency on the orchestrator.
var _ ingest.MarkerStore = (*Store)(nil)

func TestRangeIdentityFilterUsesStableMarkerIdentityAndProject(t *testing.T) {
	organisationId := primitive.NewObjectID()
	projectId := primitive.NewObjectID()
	filter := rangeIdentityFilter(bson.M{
		"value": "person", "organisationId": organisationId.Hex(), "projectId": projectId,
		"deviceId": "device-1", "groupId": "group-1", "start": int64(10), "end": int64(20),
	})
	if filter["value"] != "person" || filter["deviceId"] != "device-1" || filter["start"] != int64(10) {
		t.Fatalf("stable range identity = %#v", filter)
	}
	if _, present := filter["groupId"]; present {
		t.Fatalf("mutable groupId is part of range identity: %#v", filter)
	}
	if _, present := filter["end"]; present {
		t.Fatalf("mutable end is part of range identity: %#v", filter)
	}
	and, ok := filter["$and"].([]bson.M)
	if !ok || len(and) != 1 || !reflect.DeepEqual(and[0], models.ProjectScopeFilter(organisationId.Hex(), projectId)) {
		t.Fatalf("project identity = %#v, want %s", filter, projectId.Hex())
	}
}

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

	organisation := primitive.NewObjectID()
	projectId := models.DefaultProjectId(organisation)
	marker := models.Marker{
		Name:           "loitering-conformance",
		OrganisationId: organisation.Hex(),
		ProjectId:      &projectId,
		DeviceId:       "device-1",
		StartTimestamp: 1000,
		EndTimestamp:   1010,
		Tags:           []models.MarkerTag{{Name: "review"}},
		Events:         []models.MarkerEvent{{Name: "motion", StartTimestamp: 1000, EndTimestamp: 1010}},
		Categories:     []models.MarkerCategory{{Name: "security"}},
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

	// The marker, per-occurrence ranges and all option vocabularies carry the
	// same project so project-scoped readers cannot expose another project's
	// dropdown values.
	if got := count(t, ctx, db, MARKERS_COLLECTION, bson.M{"name": marker.Name, "projectId": projectId}); got != 1 {
		t.Errorf("markers carrying the project = %d, want 1", got)
	}
	if got := count(t, ctx, db, MARKER_OPTION_RANGES_COLLECTION, bson.M{"value": marker.Name, "projectId": projectId}); got != 1 {
		t.Errorf("ranges carrying the project = %d, want 1", got)
	}
	optionValues := map[string]string{
		MARKER_OPTIONS_COLLECTION:          marker.Name,
		MARKER_TAG_OPTIONS_COLLECTION:      "review",
		MARKER_EVENT_OPTIONS_COLLECTION:    "motion",
		MARKER_CATEGORY_OPTIONS_COLLECTION: "security",
	}
	for collection, value := range optionValues {
		if got := count(t, ctx, db, collection, bson.M{"value": value, "projectId": projectId}); got != 1 {
			t.Errorf("%s options carrying the project = %d, want 1", collection, got)
		}
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

// TestUpsertBackfillsPreRolloutMarker is the tolerant-clause regression against
// a real Mongo: a marker written before projectId existed must be refreshed and
// back-filled by the same upsert that finds it, not duplicated beside it. It is
// the organisation's DEFAULT project that owns such a marker, so that is the
// project the upsert runs under — a clause that were strict even there passes
// every other test in this file and fails this one.
func TestUpsertBackfillsPreRolloutMarker(t *testing.T) {
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

	dbName := fmt.Sprintf("markers_backfill_test_%d", time.Now().UnixNano())
	origDB := DatabaseName
	DatabaseName = dbName
	defer func() {
		DatabaseName = origDB
		_ = client.Database(dbName).Drop(context.Background())
	}()
	db := client.Database(dbName)

	organisation := primitive.NewObjectID()
	projectId := models.DefaultProjectId(organisation)
	marker := models.Marker{
		Name:           "pre-rollout",
		OrganisationId: organisation.Hex(),
		DeviceId:       "device-1",
		StartTimestamp: 1000,
		EndTimestamp:   1010,
	}

	// Seed the marker and its range in the shape they had before projectId
	// existed: no such field at all. The writer itself can no longer produce that
	// shape (it defaults the project), so the legacy state is inserted directly.
	if _, err := db.Collection(MARKERS_COLLECTION).InsertOne(ctx, bson.M{
		"name":           marker.Name,
		"organisationId": marker.OrganisationId,
		"deviceId":       marker.DeviceId,
		"startTimestamp": marker.StartTimestamp,
		"endTimestamp":   marker.EndTimestamp,
	}); err != nil {
		t.Fatalf("seed unprojected marker: %v", err)
	}
	if _, err := db.Collection(MARKER_OPTION_RANGES_COLLECTION).InsertOne(ctx, bson.M{
		"value":          marker.Name,
		"organisationId": marker.OrganisationId,
		"deviceId":       marker.DeviceId,
		"start":          marker.StartTimestamp,
		"end":            marker.EndTimestamp,
	}); err != nil {
		t.Fatalf("seed unprojected range: %v", err)
	}

	// Now the same marker through the writer, which stamps the organisation's
	// default project. It must land on the existing documents.
	if _, err := UpsertMarkerToMongodb(ctx, nil, client, marker); err != nil {
		t.Fatalf("upsert projected marker: %v", err)
	}

	if got := count(t, ctx, db, MARKERS_COLLECTION, bson.M{"name": marker.Name}); got != 1 {
		t.Errorf("marker documents = %d, want 1 (the stamped write must refresh the unstamped one)", got)
	}
	if got := count(t, ctx, db, MARKERS_COLLECTION, bson.M{"name": marker.Name, "projectId": projectId}); got != 1 {
		t.Errorf("back-filled markers = %d, want 1", got)
	}
	if got := count(t, ctx, db, MARKER_OPTION_RANGES_COLLECTION, bson.M{"value": marker.Name}); got != 1 {
		t.Errorf("range documents = %d, want 1 (the range must back-fill too, not duplicate)", got)
	}
	if got := count(t, ctx, db, MARKER_OPTION_RANGES_COLLECTION, bson.M{"value": marker.Name, "projectId": projectId}); got != 1 {
		t.Errorf("back-filled ranges = %d, want 1", got)
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

// TestMarkerSummaryEntry verifies the per-occurrence markerSummary document is
// built from the correlated marker fields and omits empty values. It needs no
// Mongo.
func TestMarkerSummaryEntry(t *testing.T) {
	t.Run("populated", func(t *testing.T) {
		marker := models.Marker{
			Name:           "person",
			StartTimestamp: 1000,
			EndTimestamp:   1010,
			Events:         []models.MarkerEvent{{Name: "motion"}, {Name: ""}},
			Tags:           []models.MarkerTag{{Name: "red"}},
			Categories:     []models.MarkerCategory{{Name: "security"}, {Name: ""}},
		}
		entry, ok := markerSummaryEntry(marker)
		if !ok {
			t.Fatal("markerSummaryEntry ok = false, want true")
		}
		if entry["name"] != "person" {
			t.Errorf("name = %v, want person", entry["name"])
		}
		if entry["startTimestamp"] != int64(1000) || entry["endTimestamp"] != int64(1010) {
			t.Errorf("timestamps = %v/%v, want 1000/1010", entry["startTimestamp"], entry["endTimestamp"])
		}
		assertNames := func(key string, want ...string) {
			got, _ := entry[key].([]string)
			if len(got) != len(want) {
				t.Fatalf("%s = %v, want %v", key, entry[key], want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("%s[%d] = %q, want %q", key, i, got[i], want[i])
				}
			}
		}
		assertNames("eventNames", "motion")
		assertNames("tagNames", "red")
		assertNames("categoryNames", "security")
	})

	t.Run("empty omitted", func(t *testing.T) {
		entry, ok := markerSummaryEntry(models.Marker{Name: "solo", StartTimestamp: 5})
		if !ok {
			t.Fatal("markerSummaryEntry ok = false, want true")
		}
		for _, key := range []string{"eventNames", "tagNames", "categoryNames", "endTimestamp"} {
			if _, present := entry[key]; present {
				t.Errorf("expected %q to be omitted, got %v", key, entry[key])
			}
		}
	})

	t.Run("nothing to record", func(t *testing.T) {
		if _, ok := markerSummaryEntry(models.Marker{}); ok {
			t.Error("markerSummaryEntry ok = true for empty marker, want false")
		}
	})
}

// TestResolveMarkerProject pins the writer's tenancy defaulting. It is the rule
// that lets a door which never resolved a project still store a project-scoped
// marker, so the regression it guards is an invisible one: a nil here writes a
// marker that is correct in every field a test usually asserts, yet vanishes
// from every project-scoped read. It needs no Mongo.
func TestResolveMarkerProject(t *testing.T) {
	organisationId := primitive.NewObjectID()

	t.Run("explicit assignment wins", func(t *testing.T) {
		projectId := primitive.NewObjectID()
		got := resolveMarkerProject(models.Marker{
			OrganisationId: organisationId.Hex(),
			ProjectId:      &projectId,
		})
		if got == nil || *got != projectId {
			t.Fatalf("resolveMarkerProject = %v, want the caller's %v", got, projectId)
		}
	})

	t.Run("unassigned falls back to the organisation default", func(t *testing.T) {
		want := models.DefaultProjectId(organisationId)
		got := resolveMarkerProject(models.Marker{OrganisationId: organisationId.Hex()})
		if got == nil || *got != want {
			t.Fatalf("resolveMarkerProject = %v, want the default %v", got, want)
		}
	})

	t.Run("zero is unassigned, not a project", func(t *testing.T) {
		// A zero ObjectID means "nobody filled this in". Honouring it would stamp
		// NilObjectID onto the marker and hide it from every project-scoped read.
		zero := primitive.NilObjectID
		want := models.DefaultProjectId(organisationId)
		got := resolveMarkerProject(models.Marker{
			OrganisationId: organisationId.Hex(),
			ProjectId:      &zero,
		})
		if got == nil || *got != want {
			t.Fatalf("resolveMarkerProject = %v, want the default %v", got, want)
		}
	})

	t.Run("non-ObjectID organisation degrades to nil", func(t *testing.T) {
		// Guessing would be worse than not stamping: an organisation-wide marker
		// is still resolved by the tolerant read predicates, a fabricated one is
		// not resolved by anything.
		if got := resolveMarkerProject(models.Marker{OrganisationId: "org-1"}); got != nil {
			t.Fatalf("resolveMarkerProject = %v, want nil for a non-ObjectID organisation", got)
		}
	})
}

func TestOptionUpsertCarriesProjectScope(t *testing.T) {
	organisation := primitive.NewObjectID()
	projectId := models.DefaultProjectId(organisation)
	marker := models.Marker{
		OrganisationId: organisation.Hex(),
		ProjectId:      &projectId,
	}

	filter := optionUpsertFilter(marker, "motion")
	if filter["value"] != "motion" || filter["organisationId"] != organisation.Hex() {
		t.Fatalf("option filter = %#v, want value and organisation identity", filter)
	}
	assertTolerantProjectScope(t, filter, projectId)

	set := optionSetDoc(1234, &projectId)
	if set["updatedAt"] != int64(1234) || set["projectId"] != projectId {
		t.Errorf("option $set = %#v, want updatedAt and projectId", set)
	}

	realProject := primitive.NewObjectID()
	marker.ProjectId = &realProject
	assertStrictProjectScope(t, optionUpsertFilter(marker, "motion"), realProject)
}

// TestMarkerSetDocCarriesProject pins what the upsert's $set must contain, and
// it is load-bearing for the identity filter above rather than a tautology about
// bson tags. For the organisation's default project the project equality is
// nested in an $in, and Mongo seeds an upserted document only from a filter's
// direct *equality* clauses — so nothing in the filter would put projectId on a fresh insert. This
// $set is the only thing that stamps it, and the only thing that back-fills a
// matched legacy marker.
func TestMarkerSetDocCarriesProject(t *testing.T) {
	organisation := primitive.NewObjectID()
	projectId := models.DefaultProjectId(organisation)

	set, err := markerSetDoc(models.Marker{
		Id:             primitive.NewObjectID(),
		Name:           "loitering",
		OrganisationId: organisation.Hex(),
		ProjectId:      &projectId,
	})
	if err != nil {
		t.Fatalf("markerSetDoc: %v", err)
	}
	if got := set["projectId"]; got != projectId {
		t.Errorf("$set projectId = %v, want %v — without it an upsert under the "+
			"tolerant $in clause inserts an unstamped marker", got, projectId)
	}
	if _, present := set["_id"]; present {
		t.Error("$set must not carry _id: the first insert owns it")
	}
}

// TestMediaLinkFilter pins the media-collection predicate used by the
// authoritative by-key path. The regression it guards is a silent one: filtering
// on media."deviceId" — a deprecated field no producer writes — matches zero
// documents and returns no error, so the marker lands with its mediaKeys while
// the media is never tagged. It needs no Mongo.
func TestMediaLinkFilter(t *testing.T) {
	organisation := primitive.NewObjectID()
	projectId := models.DefaultProjectId(organisation)
	marker := models.Marker{
		DeviceId:       "device-key-1",
		OrganisationId: organisation.Hex(),
		ProjectId:      &projectId,
	}
	filter := mediaLinkFilter(marker, "org/recording.mp4")

	if filter["videoFile"] != "org/recording.mp4" {
		t.Errorf("videoFile = %v, want org/recording.mp4", filter["videoFile"])
	}
	if filter["organisationId"] != organisation.Hex() {
		t.Errorf("organisationId = %v, want %v", filter["organisationId"], organisation.Hex())
	}
	if _, present := filter["deviceId"]; present {
		t.Error("device must not be matched on the top-level deprecated deviceId field")
	}
	// The key pins the recording, so a timestamp guard would only reintroduce
	// drift sensitivity.
	for _, key := range []string{"startTimestamp", "endTimestamp"} {
		if _, present := filter[key]; present {
			t.Errorf("by-key link must not carry a %s guard", key)
		}
	}
	assertDeviceScope(t, filter, "device-key-1")
	assertTolerantProjectScope(t, filter, projectId)

	t.Run("a real project does not tag unstamped media", func(t *testing.T) {
		// The leak this refactor closes: with an unconditionally tolerant clause a
		// second project's marker would tag every unstamped recording in the
		// organisation.
		realProject := primitive.NewObjectID()
		marker := marker
		marker.ProjectId = &realProject

		filter := mediaLinkFilter(marker, "org/recording.mp4")

		assertDeviceScope(t, filter, "device-key-1")
		assertStrictProjectScope(t, filter, realProject)
	})

	t.Run("omits absent scopes", func(t *testing.T) {
		bare := mediaLinkFilter(models.Marker{}, "org/recording.mp4")
		for _, key := range []string{"$or", "$and", "organisationId", "projectId"} {
			if _, present := bare[key]; present {
				t.Errorf("expected %q to be omitted for a marker that carries no scope", key)
			}
		}
	})
}

// TestMediaOverlapFilter pins the no-reference fallback: same device, overlapping
// span. It shares the device predicate with the by-key path, so it carries the
// same deviceKey correction.
func TestMediaOverlapFilter(t *testing.T) {
	organisation := primitive.NewObjectID()
	projectId := models.DefaultProjectId(organisation)
	filter := mediaOverlapFilter(models.Marker{
		DeviceId:       "device-key-1",
		OrganisationId: organisation.Hex(),
		ProjectId:      &projectId,
		StartTimestamp: 1000,
		EndTimestamp:   1010,
	})

	assertDeviceScope(t, filter, "device-key-1")
	assertTolerantProjectScope(t, filter, projectId)
	if filter["organisationId"] != organisation.Hex() {
		t.Errorf("organisationId = %v, want %v", filter["organisationId"], organisation.Hex())
	}
	if start, _ := filter["startTimestamp"].(bson.M); start["$lte"] != int64(1010) {
		t.Errorf("startTimestamp = %v, want $lte 1010", filter["startTimestamp"])
	}
	if end, _ := filter["endTimestamp"].(bson.M); end["$gte"] != int64(1000) {
		t.Errorf("endTimestamp = %v, want $gte 1000", filter["endTimestamp"])
	}

	// The organisation clause is unconditional here, unlike mediaLinkFilter: this
	// path has no key pinning a single recording, so an absent organisation would
	// leave a device key and a time window as the only bounds. Device keys are
	// labels, not ids — two tenants can pick the same one. "" matches nothing
	// because media.organisationId is omitempty, which is the intended
	// fail-closed outcome, so this must not be relaxed to a conditional clause.
	t.Run("a marker with no organisation tags nothing", func(t *testing.T) {
		bare := mediaOverlapFilter(models.Marker{DeviceId: "device-key-1"})
		if bare["organisationId"] != "" {
			t.Errorf("organisationId = %#v, want the unmatchable empty string", bare["organisationId"])
		}
	})
}

// TestMediaIdFilter pins the explicit-id branch. mediaIds reach
// AddMarkerToMongodb as a variadic argument on an exported API, so they are
// caller-supplied: an _id alone pins whichever document the caller named, in
// whichever tenant.
func TestMediaIdFilter(t *testing.T) {
	organisation := primitive.NewObjectID()
	projectId := models.DefaultProjectId(organisation)
	mediaId := primitive.NewObjectID()

	filter := mediaIdFilter(models.Marker{
		OrganisationId: organisation.Hex(),
		ProjectId:      &projectId,
		StartTimestamp: 1000,
		EndTimestamp:   1010,
	}, mediaId)

	if filter["_id"] != mediaId {
		t.Errorf("_id = %v, want %v", filter["_id"], mediaId)
	}
	if filter["organisationId"] != organisation.Hex() {
		t.Errorf("organisationId = %v, want %v", filter["organisationId"], organisation.Hex())
	}
	assertTolerantProjectScope(t, filter, projectId)

	t.Run("a real project does not adopt unstamped media", func(t *testing.T) {
		realProject := primitive.NewObjectID()
		filter := mediaIdFilter(models.Marker{
			OrganisationId: organisation.Hex(),
			ProjectId:      &realProject,
		}, mediaId)

		assertStrictProjectScope(t, filter, realProject)
	})
}

// TestMarkerUpsertFilterKeepsBothAxes pins the composition on the upsert identity
// filter, where getting it wrong is worst: a project clause merged over another
// operator would change which marker the upsert adopts, silently.
func TestMarkerUpsertFilterKeepsBothAxes(t *testing.T) {
	organisation := primitive.NewObjectID()
	projectId := models.DefaultProjectId(organisation)

	filter := markerUpsertFilter(models.Marker{
		OrganisationId: organisation.Hex(),
		DeviceId:       "device-1",
		Name:           "loitering",
		StartTimestamp: 1000,
		ProjectId:      &projectId,
	})

	if filter["organisationId"] != organisation.Hex() || filter["deviceId"] != "device-1" ||
		filter["name"] != "loitering" || filter["startTimestamp"] != int64(1000) {
		t.Errorf("filter = %#v, want the (organisation, device, name, start) identity intact", filter)
	}
	assertTolerantProjectScope(t, filter, projectId)
}

// projectClause returns the single project clause Apply nested under $and.
func projectClause(t *testing.T, filter bson.M) bson.M {
	t.Helper()
	if _, merged := filter["projectId"]; merged {
		t.Fatalf("filter = %#v, want the project clause under $and, not merged as a top-level key", filter)
	}
	and, ok := filter["$and"].([]bson.M)
	if !ok || len(and) != 1 {
		t.Fatalf("$and = %#v, want exactly the project clause", filter["$and"])
	}
	return and[0]
}

// assertTolerantProjectScope checks that a filter narrows to the organisation's
// DEFAULT project while still resolving media stored before the project axis
// existed. A strict clause here would silently tag nothing for every recording
// predating the project stamp, which is a linkage regression no other test would
// catch. The clause lives under $and, independently of the $or the device scope
// occupies.
func assertTolerantProjectScope(t *testing.T, filter bson.M, projectId primitive.ObjectID) {
	t.Helper()
	clause := projectClause(t, filter)
	condition, ok := clause["projectId"].(bson.M)
	if !ok {
		t.Fatalf("project clause = %#v, want a projectId condition", clause)
	}
	values, ok := condition["$in"].(bson.A)
	if !ok || len(values) != 2 || values[0] != projectId || values[1] != nil {
		t.Errorf("projectId condition = %#v, want $in [%v, nil]", condition, projectId)
	}
}

// assertStrictProjectScope is the counterpart: a project that is NOT its
// organisation's default must match only what is stamped for it. A tolerant
// clause here would reach every unstamped document in the organisation — the
// cross-project leak the previous unconditional predicate carried.
func assertStrictProjectScope(t *testing.T, filter bson.M, projectId primitive.ObjectID) {
	t.Helper()
	clause := projectClause(t, filter)
	if got := clause["projectId"]; got != projectId {
		t.Errorf("project clause = %#v, want strict equality on %v", clause, projectId)
	}
	if _, tolerant := clause["projectId"].(bson.M); tolerant {
		t.Errorf("project clause = %#v, want no tolerant arm for a non-default project", clause)
	}
}

// assertDeviceScope checks that a filter narrows to the device by the live
// deviceKey field while still resolving legacy documents that stored the key
// under deviceId.
func assertDeviceScope(t *testing.T, filter bson.M, deviceKey string) {
	t.Helper()
	scope, ok := filter["$or"].([]bson.M)
	if !ok {
		t.Fatalf("$or = %v, want a device scope", filter["$or"])
	}
	matched := map[string]bool{}
	for _, clause := range scope {
		for field, value := range clause {
			if value != deviceKey {
				t.Errorf("%s = %v, want %q", field, value, deviceKey)
			}
			matched[field] = true
		}
	}
	if !matched["deviceKey"] {
		t.Error("device scope must match media.deviceKey, the field producers actually write")
	}
	if !matched["deviceId"] {
		t.Error("device scope must still resolve legacy media that stored the key under deviceId")
	}
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
