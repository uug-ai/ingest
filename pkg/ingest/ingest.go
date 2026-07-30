// Package ingest is the transport-agnostic core that receives a producer
// result — from the API or the pipeline queue — and runs the right ordered,
// idempotent sequence of actions for its kind.
//
// "Service" here means a package, not a process: there is no new microservice,
// deployment, queue or network hop. The package is compiled *into* both the
// hub-api and the analyser binaries; each calls IngestBlocks(...) in-process on
// its own request. The package is deliberately infra-free — it only depends on
// types already in the models module and on the sink interfaces declared here,
// so it stays a fast, testable library. Concrete persistence (the Mongo
// upsert, region promotion) is supplied by each app through the Scope sinks.
//
// Routing happens on two independent axes that must not be conflated:
//
//   - Kind  (the operation) — what is this result? detection vs marker vs
//     thumbnail vs sprite. Different shapes, different sinks, different action
//     sequences. Routed here, by the handlers registry.
//   - Task  (within a kind) — which flavour? box / pose inside detection. All
//     tasks of a kind share one contract and one collection. Routed *inside*
//     the kind handler (see detection.go). A producer whose result has a
//     genuinely different shape (e.g. a marker: a timeline annotation, not a
//     detection's boxes) is its own kind, not a task.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// ErrUnknownKind is returned when an envelope carries a block whose type has no
// registered handler. IngestBlocks rejects the whole envelope rather than
// applying a partial result, so a bad block never leaves half a write behind.
var ErrUnknownKind = errors.New("ingest: no handler registered for kind")

// ErrSourceNotAllowed is returned when a block type is not permitted from the
// request's transport/trust source — e.g. an external API push trying to emit a
// pipeline-only block. It is the per-block access boundary.
var ErrSourceNotAllowed = errors.New("ingest: block type not permitted from source")

// ErrTooManyBlocks is returned when an envelope carries more blocks than
// maxBlocksPerPayload, a boundary guard against an oversized payload.
var ErrTooManyBlocks = errors.New("ingest: too many blocks in payload")

// ErrPersist tags an action (sink) failure so a caller can tell a retryable
// persistence error apart from a non-retryable decode/validation one and
// re-queue the message instead of dropping the result.
var ErrPersist = errors.New("ingest: persistence failed")

// ErrPermanent tags a sink failure that is deterministic — a schema/validator
// rejection, a duplicate-key clash on a unique index — and so can never succeed
// on redelivery. Where ErrPersist says "retry me", ErrPermanent says "drop me
// and record the result": the two together stop a single bad write from looping
// the consumer queue forever. A sink reports permanence either by wrapping
// ErrPermanent or by returning an error with a Permanent() bool method that
// returns true (the markers sink uses the method form so it need not import this
// package); IngestBlocks routes such a failure here instead of to ErrPersist.
var ErrPermanent = errors.New("ingest: permanent sink failure")

// Block types the core handles today. A producer stamps Block.Type with one of
// these; the handlers registry routes on it. A genuinely new shape adds a new
// constant plus a handler — most new producers just recombine existing types.
const (
	KindDetection  = "detection"   // a run of detection tracks/boxes (detection.go)
	KindMarker     = "marker"      // a timeline annotation, one models.Marker (marker.go)
	KindMediaPatch = "media-patch" // a partial update to one media document (media_patch.go)
)

// maxBlocksPerPayload caps how many blocks one envelope may carry.
const maxBlocksPerPayload = 64

// Source is the transport/trust axis a result arrived on. It gates the
// optional side-effects of a handler via Action.RunFor — it is NOT the
// detection provenance (that is models.DetectionSource, carried in the
// payload). An API client can never write the database itself, so it is
// always delegated; pipeline is the trusted in-cluster transport.
type Source string

const (
	// SourceAPI is an authenticated external API push. Always delegated; the
	// core write runs, trusted side-effects (e.g. auto-promote to redaction)
	// do not.
	SourceAPI Source = "api"
	// SourcePipeline is an in-cluster pipeline stage handing a result back.
	// Trusted: the full action sequence runs.
	SourcePipeline Source = "pipeline"
)

// Target identifies the recording a result attaches to. The adapter resolves
// the producer's reference (mediaKey / analysisId over the API, or the event's
// fileName over the queue) to this already-resolved identity before calling
// Ingest, so the core never performs a lookup.
type Target struct {
	// Key is the recording's stable key (media.videoFile / analysis.key).
	Key string
	// OrganisationId scopes every write to the owning organisation.
	OrganisationId string
	// DeviceId is denormalised onto stored runs for filtering / cascade cleanup.
	DeviceId string
	// RecordingTimestamp (epoch seconds) is denormalised onto the run so cleanup
	// expires it on the recording's retention clock, not the post time.
	RecordingTimestamp int64
}

// Scope carries the per-request context shared by every action: the transport
// it arrived on and the persistence sinks the app injected. Keeping the sinks
// as interfaces here is what keeps this package infra-free — the concrete
// Mongo implementations live in each app's repository layer.
type Scope struct {
	// Source is the transport/trust axis; gates Action.RunFor.
	Source Source
	// Detections is the sink for the mandatory detection-run upsert.
	Detections DetectionStore
	// Regions is the sink for the optional promote-tracks-to-redaction effect.
	Regions RegionPromoter
	// Markers is the sink for the mandatory marker upsert. Required only by an
	// app that routes the "marker" kind; nil otherwise (the action errors if a
	// marker block arrives without it).
	Markers MarkerStore
	// Media is the sink for the media-patch kind's partial update. Required only
	// by an app that routes the "media-patch" kind; nil otherwise (the action
	// errors if a media-patch block arrives without it).
	Media MediaPatcher
}

// Report is the kind-agnostic envelope every handler returns. Only the
// genuinely universal fields live here — the run's id and any non-fatal
// warnings — so adding a kind never widens this struct. The kind-specific
// summary (what and how much was stored, which items were rejected) lives in
// Detail, a value the kind owns: a queue adapter logs Detail.Summary()
// kind-agnostically, and an HTTP adapter type-asserts Detail to the concrete
// type for its response body (it already branches by kind).
type Report struct {
	RunId    string
	Warnings []string
	Detail   ReportDetail
}

// ReportDetail is a kind's own result summary. Each kind defines a concrete
// type (DetectionDetail, MarkerDetail, …). Summary keeps logging kind-agnostic;
// a caller that needs the typed counts asserts on the concrete type — it always
// knows the kind it asked Ingest to run.
type ReportDetail interface {
	// Summary is a short, human-readable line for logs, e.g.
	// "3 track(s), 50 box(es), 1 rejected".
	Summary() string
}

// Block is one self-describing unit of a producer's result. Type selects the
// handler; Data is the block's own payload (the same per-kind body the typed
// decoders validate); Schema is an optional per-block version. The same type
// may appear more than once in an envelope, so each block instance must carry
// its own stable identity (e.g. a distinct detection source.runId).
type Block struct {
	Type   string          `json:"type"`
	Schema string          `json:"schema,omitempty"`
	Data   json.RawMessage `json:"data"`
}

// BlockEnvelope is the payload a producer emits: a variable-length, possibly
// heterogeneous list of blocks. It replaces the single typed body the per-kind
// entry point took, so one result can carry e.g. a detection plus markers.
type BlockEnvelope struct {
	Schema string  `json:"schema,omitempty"`
	Blocks []Block `json:"blocks"`
}

// BlockReport is the per-block outcome: the kind's existing Report (run id,
// warnings, typed detail) tagged with the block type it came from.
type BlockReport struct {
	Type string
	Report
}

// BatchReport aggregates the per-block reports for one envelope, plus any
// envelope-level warnings.
type BatchReport struct {
	Blocks   []BlockReport
	Warnings []string
}

// Action is one idempotent effect in a handler's ordered sequence. Each action
// owns its own sink — essential because sinks genuinely differ (own collection
// vs enrich-in-place). The dispatcher only orders and gates them.
type Action interface {
	// Name identifies the action for logging / reporting.
	Name() string
	// Apply runs the effect against the already-decoded, typed run. It must be
	// idempotent (keyed upsert / keyed replace) because delivery is at-least-once.
	Apply(ctx context.Context, scope Scope, target Target, run any) error
	// RunFor gates the action by transport/trust. The mandatory persistence
	// action returns true for every source; trusted-only side-effects return
	// true only for SourcePipeline.
	RunFor(source Source) bool
}

// Handler is a kind's pipeline: a Decode step that validates + task-routes +
// normalises the payload once into a typed run, followed by the ordered
// actions that consume that run. The first action is always the mandatory
// persistence; any further actions are the optional, RunFor-gated side-effects.
type Handler struct {
	// Kind is the block type this handler processes (the registry key).
	Kind string
	// AllowedSources is the transport/trust allow-list: which sources may emit
	// this block type. Empty means trusted pipeline only — the safe default, so a
	// new block type is never accidentally writable over the external API.
	AllowedSources []Source
	Decode         func(scope Scope, target Target, payload json.RawMessage) (run any, report Report, err error)
	Actions        []Action
}

// AllowsSource reports whether a block of this kind may be ingested from src.
func (h Handler) AllowsSource(src Source) bool {
	if len(h.AllowedSources) == 0 {
		return src == SourcePipeline
	}
	for _, s := range h.AllowedSources {
		if s == src {
			return true
		}
	}
	return false
}

// handlers is the kind registry — the one switch the dispatcher routes on.
// Adding a kind is a new entry here plus its Decode/Actions; the dispatcher
// never grows a case.
var handlers = map[string]Handler{
	detectionHandler.Kind:  detectionHandler,
	markerHandler.Kind:     markerHandler,
	mediaPatchHandler.Kind: mediaPatchHandler,
	// "thumbnail": thumbnailHandler, "sprite": spriteHandler — migrated from
	// the analyser's hardcoded switch later.
}

// IngestBlocks is the single entry point a producer's result flows through. The
// payload is a self-describing BlockEnvelope: a variable-length, possibly
// heterogeneous list of blocks. It validates the envelope up front — the block
// cap, and that every block type is registered and permitted for this source —
// so an unknown or forbidden block rejects the whole result before any write,
// then decodes and applies each block in order. A sink failure is tagged
// ErrPersist so the caller can retry; a decode/validation failure is not.
func IngestBlocks(ctx context.Context, scope Scope, target Target, env BlockEnvelope) (BatchReport, error) {
	var batch BatchReport

	if len(env.Blocks) > maxBlocksPerPayload {
		return batch, fmt.Errorf("%w: %d exceeds limit of %d", ErrTooManyBlocks, len(env.Blocks), maxBlocksPerPayload)
	}

	// Pre-pass: reject an unknown or forbidden block before applying any block,
	// so a bad envelope never leaves a partial write behind.
	for i, b := range env.Blocks {
		h, ok := handlers[b.Type]
		if !ok {
			return batch, fmt.Errorf("%w: %s", ErrUnknownKind, b.Type)
		}
		if !h.AllowsSource(scope.Source) {
			return batch, fmt.Errorf("%w: %q from %q (block %d)", ErrSourceNotAllowed, b.Type, scope.Source, i)
		}
	}

	for i, b := range env.Blocks {
		h := handlers[b.Type]

		run, report, err := h.Decode(scope, target, b.Data)
		if err != nil {
			return batch, fmt.Errorf("ingest: block %d (%s): %w", i, b.Type, err)
		}

		for _, a := range h.Actions {
			if !a.RunFor(scope.Source) {
				continue
			}
			if err := a.Apply(ctx, scope, target, run); err != nil {
				// A deterministic rejection (validator/duplicate key) can only repeat
				// on redelivery, so tag it ErrPermanent — the caller drops and records
				// the result instead of re-queuing it forever. Every other sink failure
				// stays ErrPersist (retryable).
				if IsPermanent(err) {
					return batch, fmt.Errorf("%w: block %d (%s) action %q: %v", ErrPermanent, i, b.Type, a.Name(), err)
				}
				return batch, fmt.Errorf("%w: block %d (%s) action %q: %v", ErrPersist, i, b.Type, a.Name(), err)
			}
		}

		batch.Blocks = append(batch.Blocks, BlockReport{Type: b.Type, Report: report})
	}

	return batch, nil
}

// IsPermanent reports whether err is a deterministic, non-retryable sink failure
// (see ErrPermanent). It matches both signalling forms: an error that wraps
// ErrPermanent, and an error exposing a Permanent() bool method that returns
// true — the markers sink uses the method form so it need not import this
// package. IngestBlocks uses it to split a sink failure into the ErrPermanent
// (drop) and ErrPersist (retry) paths; callers may use it directly too.
func IsPermanent(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPermanent) {
		return true
	}
	var p interface{ Permanent() bool }
	if errors.As(err, &p) {
		return p.Permanent()
	}
	return false
}
