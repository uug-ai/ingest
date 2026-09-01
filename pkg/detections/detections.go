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
	"fmt"
	"time"

	"github.com/uug-ai/ingest/internal/projectfilter"
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

type detectionCollection interface {
	UpdateOne(context.Context, any, any, ...*options.UpdateOptions) (*mongo.UpdateResult, error)
}

// NewStore builds the ingest detection sink over a Mongo database.
func NewStore(db *mongo.Database) *Store {
	return &Store{db: db}
}

// UpsertDetectionRun persists the run, replacing any prior run with the same
// (key, organisationId, projectId, source.runId) so at-least-once redelivery
// stays idempotent. _id and createdAt are owned by the first insert and never
// overwritten on replace. It satisfies ingest.DetectionStore so the orchestrator
// can use it as a sink without importing this package's Mongo deps.
//
// The project clause is the shared one (models.ProjectScopeFilter, adapted by
// internal/projectfilter): tolerant of runs written before the field existed
// only when the run belongs to its organisation's default project, and strict
// for a real project so this upsert can never adopt another project's run.
// $set carries the stamped projectId explicitly — models.DetectionRun tags
// ProjectId `bson:"projectId,omitempty"`, and it must be carried because the
// tolerant clause is an $or, which Mongo does not seed an upserted document
// from. A matched legacy run is therefore refreshed and back-filled by this
// same operation rather than duplicated beside it.
func (s *Store) UpsertDetectionRun(ctx context.Context, run models.DetectionRun) error {
	allowLegacyAdoption, err := canAdoptLegacyDetection(ctx, s.db.Collection("analysis"), run)
	if err != nil {
		return err
	}
	return upsertDetectionRun(ctx, s.db.Collection(DetectionsCollection), run, allowLegacyAdoption)
}

type analysisParent struct {
	OrganisationId string              `bson:"organisationId"`
	ProjectId      *primitive.ObjectID `bson:"projectId"`
	UserId         string              `bson:"userid"`
	LegacyUserId   string              `bson:"user_id"`
}

func canAdoptLegacyDetection(ctx context.Context, coll *mongo.Collection, run models.DetectionRun) (bool, error) {
	cursor, err := coll.Find(
		ctx,
		bson.M{"key": run.Key},
		options.Find().SetLimit(2).SetProjection(bson.M{
			"organisationId": 1,
			"projectId":      1,
			"userid":         1,
			"user_id":        1,
		}),
	)
	if err != nil {
		return false, err
	}
	defer cursor.Close(ctx)

	var parents []analysisParent
	if err := cursor.All(ctx, &parents); err != nil {
		return false, err
	}
	return uniqueParentMatchesRun(parents, run), nil
}

func uniqueParentMatchesRun(parents []analysisParent, run models.DetectionRun) bool {
	return len(parents) == 1 && parentMatchesRun(parents[0], run)
}

func parentMatchesRun(parent analysisParent, run models.DetectionRun) bool {
	organisationId := parent.OrganisationId
	if organisationId == "" {
		organisationId = parent.UserId
	}
	if organisationId == "" {
		organisationId = parent.LegacyUserId
	}
	if organisationId == "" || organisationId != run.OrganisationId || run.ProjectId == nil || run.ProjectId.IsZero() {
		return false
	}

	projectId := parent.ProjectId
	if projectId == nil || projectId.IsZero() {
		ownerId, err := primitive.ObjectIDFromHex(organisationId)
		if err != nil {
			return false
		}
		defaultProjectId := models.DefaultProjectId(ownerId)
		projectId = &defaultProjectId
	}
	return *projectId == *run.ProjectId
}

func upsertDetectionRun(ctx context.Context, coll detectionCollection, run models.DetectionRun, allowLegacyAdoption bool) error {
	now := time.Now().UnixMilli()

	// Server-owned identity / denormalised fields. _id and createdAt are left
	// zero so omitempty drops them from $set and they never overwrite the
	// originals on replace ($setOnInsert seeds createdAt once).
	run.Id = primitive.NilObjectID
	run.CreatedAt = 0
	run.UpdatedAt = now

	filter := upsertFilter(run, allowLegacyAdoption)
	update := bson.M{
		"$set":         run,
		"$setOnInsert": bson.M{"createdAt": now},
	}

	_, err := coll.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if mongo.IsDuplicateKeyError(err) {
		// A concurrent writer won the race on the unique (key, source.runId)
		// index between our filter check and the upsert. Retry without upsert
		// so our $set now matches and replaces it (last write wins). A duplicate
		// owned by conflicting canonical scope does not match this filter; do not
		// report that conflict as a successful write.
		duplicateErr := err
		result, retryErr := coll.UpdateOne(ctx, filter, update)
		if retryErr != nil {
			return retryErr
		}
		if result == nil || result.MatchedCount == 0 {
			return fmt.Errorf("detections: duplicate-key retry matched no document: %w", duplicateErr)
		}
		return nil
	}
	return err
}

// upsertFilter is the run's stable identity. Exact canonical ownership takes
// precedence. A document without canonical organisation ownership may be
// adopted only when its project also matches the incoming source authority;
// that project predicate tolerates an unstamped project only for the
// organisation's deterministic default project. This back-fills legacy runs
// without allowing a non-default project or conflicting canonical owner to
// claim them.
func upsertFilter(run models.DetectionRun, allowLegacyAdoption bool) bson.M {
	filter := bson.M{
		"key":          run.Key,
		"source.runId": run.Source.RunId,
	}
	canonical := projectfilter.Apply(
		bson.M{"organisationId": run.OrganisationId},
		run.OrganisationId,
		run.ProjectId,
	)
	ownership := bson.A{canonical}
	if allowLegacyAdoption && run.OrganisationId != "" && run.ProjectId != nil && !run.ProjectId.IsZero() {
		legacy := projectfilter.Apply(
			bson.M{"organisationId": bson.M{"$in": bson.A{nil, ""}}},
			run.OrganisationId,
			run.ProjectId,
		)
		ownership = append(ownership, legacy)
	}
	filter["$or"] = ownership
	return filter
}
