package media

import (
	"context"
	"errors"
	"testing"

	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type fakeMediaCollection struct {
	calls  int
	filter any
	update any
	result *mongo.UpdateResult
	err    error
}

func (f *fakeMediaCollection) UpdateOne(_ context.Context, filter, update any, _ ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	f.calls++
	f.filter = filter
	f.update = update
	if f.err != nil {
		return nil, f.err
	}
	if f.result == nil {
		return &mongo.UpdateResult{}, nil
	}
	return f.result, nil
}

func isPermanent(err error) bool {
	var p interface{ Permanent() bool }
	return errors.As(err, &p) && p.Permanent()
}

func TestPatchMedia_RejectsEmptyOrganisation(t *testing.T) {
	collection := &fakeMediaCollection{}
	store := &Store{collection: collection}

	err := store.PatchMedia(context.Background(), "  ", nil, primitive.NewObjectID().Hex(), "", map[string]any{"star": true})
	if !isPermanent(err) {
		t.Fatalf("err = %v, want permanent failure", err)
	}
	if collection.calls != 0 {
		t.Errorf("empty organisation must not reach Mongo, got %d call(s)", collection.calls)
	}
}

func TestPatchMedia_UsesMandatoryOrganisationScope(t *testing.T) {
	collection := &fakeMediaCollection{result: &mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 1}}
	store := &Store{collection: collection}
	id := primitive.NewObjectID()

	err := store.PatchMedia(context.Background(), "org-1", nil, id.Hex(), "", map[string]any{"star": true})
	if err != nil {
		t.Fatalf("PatchMedia: %v", err)
	}
	if collection.calls != 1 {
		t.Fatalf("want 1 Mongo call, got %d", collection.calls)
	}
	filter, ok := collection.filter.(bson.M)
	if !ok {
		t.Fatalf("filter type = %T, want bson.M", collection.filter)
	}
	if filter["_id"] != id {
		t.Errorf("filter _id = %v, want %v", filter["_id"], id)
	}
	if filter["organisationId"] != "org-1" {
		t.Errorf("filter organisationId = %v, want org-1", filter["organisationId"])
	}
	if _, scoped := filter["projectId"]; scoped {
		t.Error("a nil project must leave the patch organisation-wide, not add a projectId clause")
	}
}

func TestPatchMedia_ScopesToProject(t *testing.T) {
	organisation := primitive.NewObjectID()

	t.Run("the default project still reaches unstamped media", func(t *testing.T) {
		collection := &fakeMediaCollection{result: &mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 1}}
		store := &Store{collection: collection}
		projectId := models.DefaultProjectId(organisation)

		err := store.PatchMedia(context.Background(), organisation.Hex(), &projectId, primitive.NewObjectID().Hex(), "", map[string]any{"star": true})
		if err != nil {
			t.Fatalf("PatchMedia: %v", err)
		}
		filter, ok := collection.filter.(bson.M)
		if !ok {
			t.Fatalf("filter type = %T, want bson.M", collection.filter)
		}
		clause := projectClause(t, filter)
		condition, ok := clause["projectId"].(bson.M)
		if !ok {
			t.Fatalf("project clause = %#v, want a projectId condition", clause)
		}
		// The null value is what keeps media stored before the project axis
		// existed patchable — without it every pre-rollout recording becomes a
		// permanent "not found" and its patch is dropped rather than retried.
		values, ok := condition["$in"].(bson.A)
		if !ok || len(values) != 2 || values[0] != projectId || values[1] != nil {
			t.Errorf("projectId condition = %#v, want $in [%v, nil]", condition, projectId)
		}
		if filter["organisationId"] != organisation.Hex() {
			t.Errorf("filter organisationId = %v, want %v (the project narrows, it never replaces the org)", filter["organisationId"], organisation.Hex())
		}
	})

	t.Run("a real project does not reach unstamped media", func(t *testing.T) {
		// Unstamped media belongs to the organisation's default project. An
		// unconditionally tolerant clause would let a second project patch it —
		// a cross-project write no test written during the rollout would catch.
		collection := &fakeMediaCollection{result: &mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 1}}
		store := &Store{collection: collection}
		projectId := primitive.NewObjectID()

		err := store.PatchMedia(context.Background(), organisation.Hex(), &projectId, primitive.NewObjectID().Hex(), "", map[string]any{"star": true})
		if err != nil {
			t.Fatalf("PatchMedia: %v", err)
		}
		filter, ok := collection.filter.(bson.M)
		if !ok {
			t.Fatalf("filter type = %T, want bson.M", collection.filter)
		}
		clause := projectClause(t, filter)
		if clause["projectId"] != projectId {
			t.Errorf("project clause = %#v, want strict equality on %v", clause, projectId)
		}
		if _, tolerant := clause["$or"]; tolerant {
			t.Errorf("project clause = %#v, want no tolerant arm for a non-default project", clause)
		}
	})
}

// projectClause returns the single project clause the media filter nests under
// $and. The clause is composed there rather than merged in so it can carry its
// own $or without colliding with anything else in the filter.
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

func TestPatchMedia_UnmatchedTargetIsPermanent(t *testing.T) {
	collection := &fakeMediaCollection{result: &mongo.UpdateResult{MatchedCount: 0}}
	store := &Store{collection: collection}

	err := store.PatchMedia(context.Background(), "org-1", nil, primitive.NewObjectID().Hex(), "", map[string]any{"star": true})
	if !isPermanent(err) {
		t.Fatalf("err = %v, want permanent failure", err)
	}
}

func TestPatchMedia_TransientWriteErrorRemainsRetryable(t *testing.T) {
	collection := &fakeMediaCollection{err: errors.New("mongo unavailable")}
	store := &Store{collection: collection}

	err := store.PatchMedia(context.Background(), "org-1", nil, primitive.NewObjectID().Hex(), "", map[string]any{"star": true})
	if err == nil {
		t.Fatal("want write error")
	}
	if isPermanent(err) {
		t.Fatalf("transient error was marked permanent: %v", err)
	}
}

func TestPatchMedia_FallsBackToKey(t *testing.T) {
	collection := &fakeMediaCollection{result: &mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 1}}
	store := &Store{collection: collection}

	err := store.PatchMedia(context.Background(), "org-1", nil, "", "recording-key.mp4", map[string]any{"metadata.description": "Number plate detected: ABC123"})
	if err != nil {
		t.Fatalf("PatchMedia: %v", err)
	}
	filter, ok := collection.filter.(bson.M)
	if !ok {
		t.Fatalf("filter type = %T, want bson.M", collection.filter)
	}
	if filter["videoFile"] != "recording-key.mp4" {
		t.Errorf("filter videoFile = %v, want recording-key.mp4", filter["videoFile"])
	}
	if filter["organisationId"] != "org-1" {
		t.Errorf("filter organisationId = %v, want org-1 (key path must stay org-scoped)", filter["organisationId"])
	}
	if _, hasId := filter["_id"]; hasId {
		t.Error("key-targeted patch must not carry an _id filter")
	}
}

func TestPatchMedia_PrefersIdWhenBothSupplied(t *testing.T) {
	collection := &fakeMediaCollection{result: &mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 1}}
	store := &Store{collection: collection}
	id := primitive.NewObjectID()

	err := store.PatchMedia(context.Background(), "org-1", nil, id.Hex(), "recording-key.mp4", map[string]any{"star": true})
	if err != nil {
		t.Fatalf("PatchMedia: %v", err)
	}
	filter, ok := collection.filter.(bson.M)
	if !ok {
		t.Fatalf("filter type = %T, want bson.M", collection.filter)
	}
	if filter["_id"] != id {
		t.Errorf("filter _id = %v, want %v (id is primary)", filter["_id"], id)
	}
	if _, hasKey := filter["videoFile"]; hasKey {
		t.Error("id-targeted patch must not also filter by key")
	}
}

func TestPatchMedia_RequiresAnIdentifier(t *testing.T) {
	collection := &fakeMediaCollection{}
	store := &Store{collection: collection}

	err := store.PatchMedia(context.Background(), "org-1", nil, "  ", "  ", map[string]any{"star": true})
	if !isPermanent(err) {
		t.Fatalf("err = %v, want permanent failure when neither id nor key is supplied", err)
	}
	if collection.calls != 0 {
		t.Errorf("an identifier-less patch must not reach Mongo, got %d call(s)", collection.calls)
	}
}
