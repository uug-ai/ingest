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

	err := store.PatchMedia(context.Background(), "  ", primitive.NewObjectID().Hex(), map[string]any{"star": true})
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

	err := store.PatchMedia(context.Background(), "org-1", id.Hex(), map[string]any{"star": true})
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
}

func TestPatchMedia_UnmatchedTargetIsPermanent(t *testing.T) {
	collection := &fakeMediaCollection{result: &mongo.UpdateResult{MatchedCount: 0}}
	store := &Store{collection: collection}

	err := store.PatchMedia(context.Background(), "org-1", primitive.NewObjectID().Hex(), map[string]any{"star": true})
	if !isPermanent(err) {
		t.Fatalf("err = %v, want permanent failure", err)
	}
}

func TestPatchMedia_TransientWriteErrorRemainsRetryable(t *testing.T) {
	collection := &fakeMediaCollection{err: errors.New("mongo unavailable")}
	store := &Store{collection: collection}

	err := store.PatchMedia(context.Background(), "org-1", primitive.NewObjectID().Hex(), map[string]any{"star": true})
	if err == nil {
		t.Fatal("want write error")
	}
	if isPermanent(err) {
		t.Fatalf("transient error was marked permanent: %v", err)
	}
}
