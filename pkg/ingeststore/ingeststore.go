// Package ingeststore is the Mongo wiring layer for the ingest sinks. It is the
// single place that constructs the concrete, Mongo-backed implementations of the
// orchestrator's sink interfaces (ingest.DetectionStore, ingest.MarkerStore, …),
// so a composition root wires an ingest.Scope by importing ONE package instead of
// reaching into every sink package by hand.
//
// It sits one layer ABOVE the infra-free orchestrator: this package may import
// mongo, trace, and every concrete sink, which keeps ingest/pkg/ingest itself
// free of any database dependency (the orchestrator only knows the interfaces).
// When a new block type gains a shared Mongo writer, add its constructor here and
// every consumer picks it up without a new import.
package ingeststore

import (
	"github.com/uug-ai/ingest/pkg/detections"
	"github.com/uug-ai/ingest/pkg/ingest"
	"github.com/uug-ai/ingest/pkg/markers"
	"github.com/uug-ai/ingest/pkg/media"
	"github.com/uug-ai/trace/pkg/opentelemetry"
	"go.mongodb.org/mongo-driver/mongo"
)

// NewDetectionStore builds the Mongo detection-run sink (ingest.DetectionStore)
// over the caller's database. It upserts each run by identity
// (key, organisationId, projectId, source.runId) so an at-least-once redelivery
// refreshes rather than duplicates it.
func NewDetectionStore(db *mongo.Database) ingest.DetectionStore {
	return detections.NewStore(db)
}

// NewMarkerStore builds the Mongo marker sink (ingest.MarkerStore) over a Mongo
// client. tracer may be nil, in which case the writes run without a child span.
// It upserts each marker by its stable identity together with the denormalised
// lookup projections, so a redelivery refreshes rather than duplicates it.
func NewMarkerStore(client *mongo.Client, tracer *opentelemetry.Tracer) ingest.MarkerStore {
	return markers.NewStore(client, tracer)
}

// NewMediaStore builds the Mongo media-patch sink (ingest.MediaPatcher) over the
// caller's database. It applies an organisation- and project-scoped $set to the
// target media document, so a media-patch block from a workflow stage enriches a
// recording without ever crossing an organisation boundary.
func NewMediaStore(db *mongo.Database) ingest.MediaPatcher {
	return media.NewStore(db)
}
