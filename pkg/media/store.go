// Package media applies partial updates to media documents in the shared "media"
// collection. It is the concrete sink the workflow engine's ingest core injects
// for the media-patch block kind: a stage that has enriched a recording (a
// description, a star, extra tags) hands the change back and it lands here as an
// org-scoped $set.
//
// It is the media-patch counterpart of the markers and detections packages. The
// package deliberately does NOT import the ingest orchestrator — Store satisfies
// ingest.MediaPatcher structurally, so the orchestrator stays infra-free while a
// consumer injects this writer.
package media

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MediaCollection is the collection media documents live in, shared with hub-api.
const MediaCollection = "media"

// timeout bounds each media update so a slow write cannot hold a queue worker.
const timeout = 10 * time.Second

// Store is the ingest media-patch sink: it adapts an org-scoped $set update to
// the ingest.MediaPatcher interface. It holds the Mongo database media documents
// are stored in.
type Store struct {
	collection mediaCollection
}

type mediaCollection interface {
	UpdateOne(context.Context, any, any, ...*options.UpdateOptions) (*mongo.UpdateResult, error)
}

// NewStore builds the ingest media-patch sink over a Mongo database.
func NewStore(db *mongo.Database) *Store {
	return &Store{collection: db.Collection(MediaCollection)}
}

// PatchMedia applies fields ($set) to the media document identified within
// organisationId by mediaId (its _id, when non-empty) or otherwise by mediaKey
// (its recording key, media.videoFile). The update is always scoped to
// organisationId so a stage can never patch a recording another organisation
// owns, and it is idempotent (setting the same values again is a no-op). An empty
// organisation, no usable identifier, an unparseable id, or unknown/foreign media
// is a deterministic, non-retryable failure: none can be repaired by redelivery,
// and reporting success would let the workflow advertise a patch that never
// landed. It satisfies ingest.MediaPatcher so the orchestrator can use it as a
// sink without importing this package's Mongo deps.
func (s *Store) PatchMedia(ctx context.Context, organisationId, mediaId, mediaKey string, fields map[string]any) error {
	if strings.TrimSpace(organisationId) == "" {
		return permanentWriteError{err: errors.New("media-patch: organisation id is required")}
	}
	if len(fields) == 0 {
		return nil
	}

	// mediaId (the _id) is the primary target; mediaKey (media.videoFile) is the
	// fallback for a DB-free stage that only knows the recording key. Both paths
	// stay scoped to organisationId so the org can never be escaped.
	filter := bson.M{"organisationId": organisationId}
	target := strings.TrimSpace(mediaId)
	switch {
	case target != "":
		oid, err := primitive.ObjectIDFromHex(target)
		if err != nil {
			// A malformed id repeats on every redelivery, so it is permanent: drop
			// and record rather than re-queue forever.
			return permanentWriteError{err: fmt.Errorf("media-patch: invalid media id %q: %w", mediaId, err)}
		}
		filter["_id"] = oid
	default:
		key := strings.TrimSpace(mediaKey)
		if key == "" {
			return permanentWriteError{err: errors.New("media-patch: a media id or key is required")}
		}
		filter["videoFile"] = key
		target = "key " + key
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result, err := s.collection.UpdateOne(ctx, filter, bson.M{"$set": fields})
	if err != nil {
		return classifyWriteError(err)
	}
	if result.MatchedCount == 0 {
		return permanentWriteError{err: fmt.Errorf("media-patch: media %q was not found in the target organisation", target)}
	}
	return nil
}

// permanentWriteError marks a Mongo write failure the ingest core must not retry:
// a deterministic rejection (a malformed id, a document-validation failure) an
// at-least-once redelivery can only repeat. It exposes Permanent() bool so the
// ingest core recognises it WITHOUT this package importing the orchestrator
// (ingest.IsPermanent checks the behaviour). It unwraps to the underlying error.
type permanentWriteError struct{ err error }

func (e permanentWriteError) Error() string   { return e.err.Error() }
func (e permanentWriteError) Unwrap() error   { return e.err }
func (e permanentWriteError) Permanent() bool { return true }

// classifyWriteError tags a deterministic Mongo write rejection as permanent and
// leaves every other failure as a plain (retryable) error, mirroring the markers
// sink: a document-validation failure is identical on every redelivery, whereas a
// timeout, a primary step-down or a dropped connection is transient. nil passes
// through.
func classifyWriteError(err error) error {
	if err == nil {
		return nil
	}
	if isPermanentWriteError(err) {
		return permanentWriteError{err: err}
	}
	return err
}

// isPermanentWriteError reports whether a Mongo write error is deterministic. It
// is conservative on purpose: only duplicate-key and document-validation failures
// are treated as permanent, because a false "permanent" drops a recoverable write
// whereas a false "transient" merely retries — so anything ambiguous stays
// retryable.
func isPermanentWriteError(err error) bool {
	if mongo.IsDuplicateKeyError(err) {
		return true
	}
	const docValidationFailure = 121
	var we mongo.WriteException
	if errors.As(err, &we) {
		for _, w := range we.WriteErrors {
			if w.Code == docValidationFailure {
				return true
			}
		}
	}
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		if ce.Code == docValidationFailure {
			return true
		}
	}
	return false
}
