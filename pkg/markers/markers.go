// Package markers writes user-facing markers and their denormalised lookup
// collections into Mongo. It is the single home for the Hub's marker side
// effects: every door that produces a marker — the media UI (hub-api), the
// analysers, alert pipelines, and the workflow engine's ingest core — writes
// through here so a marker always lands with the same option/range/category
// projections and the same media tagging.
//
// Two write modes share one code path:
//
//   - AddMarkerToMongodb inserts a fresh marker (a new _id every call). It is
//     for authoring doors where two identical requests are two distinct markers
//     (e.g. a user adding the same annotation twice on purpose).
//   - UpsertMarkerToMongodb upserts the marker by its stable identity
//     (organisationId, deviceId, name, startTimestamp) and makes the denormalised
//     range documents idempotent too. It is for the ingest core, whose delivery
//     is at-least-once: a redelivery or a re-analysis of the same recording must
//     refresh the marker, not duplicate it.
//
// The package deliberately does NOT import the ingest orchestrator. Store
// satisfies ingest.MarkerStore structurally, so the orchestrator can stay
// infra-free while the engine injects this concrete writer as its marker sink.
package markers

import (
	"context"
	"errors"

	"github.com/uug-ai/models/pkg/models"
	"github.com/uug-ai/trace/pkg/opentelemetry"
	"go.mongodb.org/mongo-driver/mongo"
)

// Marker is the stateless authoring entry point retained for the direct callers
// (hub-api, analysers, alert pipelines, cli) that create markers outside the
// ingest core.
type Marker struct {
	// Define marker fields here
}

// New returns an authoring handle.
func New() *Marker {
	return &Marker{}
}

// Create validates and inserts a single marker, computing its duration. It uses
// the insert path: each call creates a distinct marker document.
func (m *Marker) Create(ctxTracer context.Context, tracer *opentelemetry.Tracer,
	client *mongo.Client, marker models.Marker, mediaIds ...string) (models.Marker,
	error) {

	// We require a marker name to be set, as this is used to identify the marker.
	if marker.Name == "" {
		return models.Marker{}, errors.New("marker name is required")
	}

	// Set the duration, difference between start and end time
	marker.Duration = marker.EndTimestamp - marker.StartTimestamp

	// Add the marker to the database
	insertedMarker, err := AddMarkerToMongodb(ctxTracer, tracer, client, marker, mediaIds...)
	if err != nil {
		return models.Marker{}, err
	}

	return insertedMarker, nil
}

// Store is the ingest marker sink: it adapts the idempotent marker writer to the
// ingest.MarkerStore interface (UpsertMarkers(ctx, []models.Marker) error). It
// holds the Mongo client and an optional tracer; the tracer may be nil, in which
// case the writes run without a child span (the engine has no per-result tracer
// to thread, and the marker write was untraced before this change).
type Store struct {
	client *mongo.Client
	tracer *opentelemetry.Tracer
}

// NewStore builds the ingest marker sink over a Mongo client. tracer may be nil.
func NewStore(client *mongo.Client, tracer *opentelemetry.Tracer) *Store {
	return &Store{client: client, tracer: tracer}
}

// UpsertMarkers writes each marker idempotently (keyed by identity) together
// with its denormalised projections. It satisfies ingest.MarkerStore so the
// orchestrator can use it as a sink without importing this package's Mongo deps.
func (s *Store) UpsertMarkers(ctx context.Context, markers []models.Marker) error {
	for i := range markers {
		if _, err := UpsertMarkerToMongodb(ctx, s.tracer, s.client, markers[i]); err != nil {
			return classifyWriteError(err)
		}
	}
	return nil
}

// permanentWriteError marks a Mongo write failure the ingest core must not retry:
// a deterministic rejection (a duplicate-key clash on a unique index, a document
// validation / JSON-schema failure) that an at-least-once redelivery can only
// repeat. It exposes Permanent() bool so the ingest core can recognise it WITHOUT
// this package importing the orchestrator (ingest.IsPermanent checks the
// behaviour, mirroring net.Error.Temporary). It unwraps to the underlying Mongo
// error, so callers that only test err != nil — every caller outside the ingest
// core — are unaffected.
type permanentWriteError struct{ err error }

func (e permanentWriteError) Error() string  { return e.err.Error() }
func (e permanentWriteError) Unwrap() error   { return e.err }
func (e permanentWriteError) Permanent() bool { return true }

// classifyWriteError tags a deterministic Mongo write rejection as permanent and
// leaves every other failure as a plain (retryable) error. A duplicate-key clash
// or a document-validation failure is identical on every redelivery, so the
// ingest core drops and records the result instead of looping the queue; a
// timeout, a primary step-down or a dropped connection is transient and stays
// retryable. nil passes through.
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
// is conservative on purpose: only duplicate-key and document-validation
// failures are treated as permanent, because a false "permanent" drops a
// recoverable write whereas a false "transient" merely retries (bounded, then
// dead-lettered) — so anything ambiguous is left retryable.
func isPermanentWriteError(err error) bool {
	// Duplicate key on a unique index (codes 11000/11001/12582): the same key
	// clashes on every redelivery.
	if mongo.IsDuplicateKeyError(err) {
		return true
	}
	// Document validation / JSON-schema validator rejection. Code 121.
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
