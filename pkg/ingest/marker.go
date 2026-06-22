package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// --- Sink (implemented by each app's repository; kept as an interface so this
// package stays infra-free) -------------------------------------------------

// MarkerStore is the mandatory sink for a marker block. The implementation MUST
// upsert by a stable identity — (OrganisationId, DeviceId, Name, StartTimestamp)
// — so an at-least-once redelivery or a re-analysis of the same recording
// refreshes the marker rather than duplicating it.
type MarkerStore interface {
	UpsertMarkers(ctx context.Context, markers []models.Marker) error
}

// ErrMarkerValidation tags a non-retryable marker-block validation failure (a
// missing name or start): redelivery cannot fix it, so the caller drops the
// block rather than re-queuing it.
var ErrMarkerValidation = errors.New("ingest: invalid marker block")

// --- Kind handler ----------------------------------------------------------

// markerHandler is the timeline-marker kind. Its sequence is the single
// mandatory UpsertMarker: a marker maps one-to-one onto the stored
// models.Marker, with no trusted-only side effects. AllowedSources is empty, so
// it defaults to the trusted pipeline only — a marker is a derived annotation a
// stage emits, not something an external API push writes directly. Open it to
// SourceAPI here if a future client should post markers.
var markerHandler = Handler{
	Kind:    KindMarker,
	Decode:  decodeMarker,
	Actions: []Action{UpsertMarker{}},
}

// MarkerDetail is the marker kind's ReportDetail: how many markers the block
// stored (always one — a marker block carries a single annotation).
type MarkerDetail struct {
	Stored int
}

// Summary implements ReportDetail.
func (d MarkerDetail) Summary() string {
	return fmt.Sprintf("%d marker(s)", d.Stored)
}

// decodeMarker unmarshals the block payload into a models.Marker, stamps the
// target-derived identity onto it (the wire never owns the organisation or the
// document id), fills the derived duration, and validates the minimum a timeline
// marker needs: a name and a start. It runs once per block; UpsertMarker
// consumes its typed output.
func decodeMarker(_ Scope, target Target, payload json.RawMessage) (any, Report, error) {
	var m models.Marker
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, Report{}, fmt.Errorf("ingest: decode marker payload: %w", err)
	}

	// Server-owned identity: never trust the wire for these. The sink owns _id;
	// the target owns the organisation and (when the wire omits it) the device.
	m.Id = primitive.NilObjectID
	m.OrganisationId = target.OrganisationId
	if strings.TrimSpace(m.DeviceId) == "" {
		m.DeviceId = target.DeviceId
	}

	// --- validation ---
	m.Name = strings.TrimSpace(m.Name)
	if m.Name == "" {
		return nil, Report{}, fmt.Errorf("%w: name is required", ErrMarkerValidation)
	}
	if len(m.Name) > maxNameLen {
		return nil, Report{}, fmt.Errorf("%w: name exceeds %d characters", ErrMarkerValidation, maxNameLen)
	}
	if m.StartTimestamp <= 0 {
		return nil, Report{}, fmt.Errorf("%w: startTimestamp is required", ErrMarkerValidation)
	}
	// A point-in-time marker (no end, or an end before the start) is valid:
	// collapse it to its start so the stored span is never negative.
	if m.EndTimestamp < m.StartTimestamp {
		m.EndTimestamp = m.StartTimestamp
	}
	m.Duration = m.EndTimestamp - m.StartTimestamp

	report := Report{RunId: m.Name, Detail: MarkerDetail{Stored: 1}}
	return m, report, nil
}

// --- Action ----------------------------------------------------------------

// UpsertMarker is the marker kind's mandatory persistence action: it writes the
// one decoded marker through the MarkerStore. It runs for every source.
type UpsertMarker struct{}

// Name implements Action.
func (UpsertMarker) Name() string { return "upsert_marker" }

// RunFor implements Action: persistence is mandatory, so it runs for every source.
func (UpsertMarker) RunFor(Source) bool { return true }

// Apply implements Action: it persists the decoded marker. The MarkerStore must
// be wired by any transport that routes the marker kind.
func (UpsertMarker) Apply(ctx context.Context, scope Scope, _ Target, run any) error {
	m, ok := run.(models.Marker)
	if !ok {
		return fmt.Errorf("ingest: upsert expected models.Marker, got %T", run)
	}
	if scope.Markers == nil {
		return errors.New("ingest: no MarkerStore configured on scope")
	}
	return scope.Markers.UpsertMarkers(ctx, []models.Marker{m})
}
