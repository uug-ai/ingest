package media

import (
	"context"
	"errors"
	"testing"

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

func TestPatchMedia_ScopesToProjectTolerantly(t *testing.T) {
	collection := &fakeMediaCollection{result: &mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 1}}
	store := &Store{collection: collection}
	projectId := primitive.NewObjectID()

	err := store.PatchMedia(context.Background(), "org-1", &projectId, primitive.NewObjectID().Hex(), "", map[string]any{"star": true})
	if err != nil {
		t.Fatalf("PatchMedia: %v", err)
	}
	filter, ok := collection.filter.(bson.M)
	if !ok {
		t.Fatalf("filter type = %T, want bson.M", collection.filter)
	}
	predicate, ok := filter["projectId"].(bson.M)
	if !ok {
		t.Fatalf("filter projectId = %#v, want a bson.M predicate", filter["projectId"])
	}
	values, ok := predicate["$in"].(bson.A)
	if !ok {
		t.Fatalf("projectId predicate = %#v, want an $in", predicate)
	}
	// The null arm is what keeps media stored before the project axis existed
	// patchable — without it every pre-rollout recording becomes a permanent
	// "not found" and its patch is dropped rather than retried.
	if len(values) != 2 || values[0] != projectId || values[1] != nil {
		t.Errorf("projectId $in = %#v, want [%v <nil>]", values, projectId)
	}
	if filter["organisationId"] != "org-1" {
		t.Errorf("filter organisationId = %v, want org-1 (the project narrows, it never replaces the org)", filter["organisationId"])
	}
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
