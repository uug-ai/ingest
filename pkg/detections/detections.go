// Package detections writes detection runs into the shared "detections"
// collection. It is the shared home for the Hub's detection-run side effect: the
// doors that produce a detection run write through one idempotent writer so a run
// posted over the API, one ingested by the analysis pipeline, and one ingested by
// a workflow stage all land in the same place keyed by the same identity.
//
// It is the detection counterpart of the markers package: a concrete sink the
// workflow engine's ingest core injects. The package deliberately does NOT import
// the ingest orchestrator — Store satisfies ingest.DetectionStore structurally,
// so the orchestrator stays infra-free while a consumer injects this writer.
package detections

import (
	"context"
	"time"

	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// DetectionsCollection is the dedicated collection detection runs are stored in,
// shared with hub-api and the analyser so a run posted over the API, one ingested
// by the analysis pipeline, and one ingested by a workflow stage all land in the
// same place.
const DetectionsCollection = "detections"

// Store is the ingest detection sink: it adapts the idempotent detection-run
// writer to the ingest.DetectionStore interface
// (UpsertDetectionRun(ctx, models.DetectionRun) error). It holds the Mongo
// database the runs are written into.
type Store struct {
	db *mongo.Database
}

// NewStore builds the ingest detection sink over a Mongo database.
func NewStore(db *mongo.Database) *Store {
	return &Store{db: db}
}

// UpsertDetectionRun persists the run, replacing any prior run with the same
// (key, organisationId, source.runId) so at-least-once redelivery stays
// idempotent. _id and createdAt are owned by the first insert and never
// overwritten on replace. It satisfies ingest.DetectionStore so the orchestrator
// can use it as a sink without importing this package's Mongo deps.
func (s *Store) UpsertDetectionRun(ctx context.Context, run models.DetectionRun) error {
	coll := s.db.Collection(DetectionsCollection)
	now := time.Now().UnixMilli()

	// Server-owned identity / denormalised fields. _id and createdAt are left
	// zero so omitempty drops them from $set and they never overwrite the
	// originals on replace ($setOnInsert seeds createdAt once).
	run.Id = primitive.NilObjectID
	run.CreatedAt = 0
	run.UpdatedAt = now

	filter := bson.M{
		"key":            run.Key,
		"organisationId": run.OrganisationId,
		"source.runId":   run.Source.RunId,
	}
	update := bson.M{
		"$set":         run,
		"$setOnInsert": bson.M{"createdAt": now},
	}

	_, err := coll.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if mongo.IsDuplicateKeyError(err) {
		// A concurrent writer won the race on the unique (key, source.runId)
		// index between our filter check and the upsert. Retry without upsert
		// so our $set now matches and replaces it (last write wins).
		_, err = coll.UpdateOne(ctx, filter, update)
	}
	return err
}
