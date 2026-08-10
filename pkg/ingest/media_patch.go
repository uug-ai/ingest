package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// --- Sink (implemented by each app's repository; kept as an interface so this
// package stays infra-free) -------------------------------------------------

// MediaPatcher is the mandatory sink for a media-patch block. The implementation
// MUST scope the update to the caller's organisation (so a stage can never patch
// a recording it does not own) and MUST be idempotent — a media-patch sets the
// same fields to the same values on every at-least-once redelivery, so a replay
// is a harmless no-op. A patch whose media id resolves to no owned document is a
// deterministic, non-retryable error so the caller does not advertise a patch
// that never landed.
type MediaPatcher interface {
	// PatchMedia applies fields ($set) to the media document identified within
	// organisationId by mediaId (its _id) when non-empty, otherwise by mediaKey
	// (its recording key). The decode step guarantees at least one identifier is
	// non-empty and prefers the id. fields is keyed by the media document's own
	// (dot-notation) field paths, already validated against the patchable allow
	// list by the decode step.
	PatchMedia(ctx context.Context, organisationId, mediaId, mediaKey string, fields map[string]any) error
}

// ErrMediaPatchValidation tags a non-retryable media-patch validation failure (a
// missing/invalid media id, an unknown field, an empty patch): redelivery cannot
// fix it, so the caller drops the block rather than re-queuing it.
var ErrMediaPatchValidation = errors.New("ingest: invalid media-patch block")

// mediaIdField and mediaKeyField are the reserved keys that name the target media
// document: mediaIdField by its _id (the primary target) and mediaKeyField by its
// recording key (media.videoFile, the fallback). Every other key in the block is
// a field to patch. A DB-free pipeline stage that only knows the recording key —
// it never resolves the _id — supplies mediaKey instead, mirroring how the
// marker/detection sinks already link a recording by its key.
const (
	mediaIdField  = "mediaId"
	mediaKeyField = "mediaKey"
)

// maxMediaPatchFields caps how many fields one media-patch block may set — a
// boundary guard against an oversized payload.
const maxMediaPatchFields = 32

// mediaPatchableFields is the allow-list of fields a media-patch block may set,
// mapping the wire field name to the media document's own (dot-notation) bson
// path. It is deliberately closed: identity and RBAC fields (_id, organisationId,
// deviceId, siteId, groupId) are never patchable, so a block can never re-scope a
// recording or overwrite its ownership. Add a new patchable field here.
var mediaPatchableFields = map[string]string{
	"description": "metadata.description",
	"star":        "star",
	"tagNames":    "tagNames",
	"eventNames":  "eventNames",
	"markerNames": "markerNames",
}

// --- Kind handler ----------------------------------------------------------

// mediaPatchHandler is the media-patch kind: a partial update to a single media
// document. Its sequence is the single mandatory PatchMedia. AllowedSources is
// empty, so it defaults to the trusted pipeline only — a media-patch is emitted
// by a workflow stage that has already resolved which recording to enrich, not
// something an external API push writes directly.
var mediaPatchHandler = Handler{
	Kind:    KindMediaPatch,
	Decode:  decodeMediaPatch,
	Actions: []Action{PatchMedia{}},
}

// MediaPatch is the media-patch kind's typed run: the target media id and/or key
// and the already-validated, org-scoped $set field map (keyed by media document
// paths). MediaId is the primary target; MediaKey is the fallback when the
// emitter only knows the recording key. At least one is non-empty.
type MediaPatch struct {
	MediaId  string
	MediaKey string
	Set      map[string]any
}

// MediaPatchDetail is the media-patch kind's ReportDetail: how many fields the
// block set on the media document.
type MediaPatchDetail struct {
	Fields int
}

// Summary implements ReportDetail.
func (d MediaPatchDetail) Summary() string {
	return fmt.Sprintf("patched %d media field(s)", d.Fields)
}

// decodeMediaPatch unmarshals the block payload into a media id plus a flat set
// of field/value pairs. It requires a syntactically valid media id and at least
// one field, rejects any field outside the patchable allow-list, and maps each
// wire field name to its media document path. It runs once per block; PatchMedia
// consumes its typed output.
func decodeMediaPatch(_ Scope, target Target, payload json.RawMessage) (any, Report, error) {
	if strings.TrimSpace(target.OrganisationId) == "" {
		return nil, Report{}, fmt.Errorf("%w: target organisationId is required", ErrMediaPatchValidation)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, Report{}, fmt.Errorf("ingest: decode media-patch payload: %w", err)
	}

	mediaId, err := optionalStringField(raw, mediaIdField)
	if err != nil {
		return nil, Report{}, err
	}
	mediaKey, err := optionalStringField(raw, mediaKeyField)
	if err != nil {
		return nil, Report{}, err
	}
	// mediaId is the primary target: when present it names a document by its _id, so
	// reject a malformed one here (a non-retryable validation failure) so the sink
	// never has to. mediaKey is the fallback for a stage that only knows the
	// recording key; it is an opaque string the sink resolves.
	if mediaId != "" {
		if _, err := primitive.ObjectIDFromHex(mediaId); err != nil {
			return nil, Report{}, fmt.Errorf("%w: %s %q is not a valid id", ErrMediaPatchValidation, mediaIdField, mediaId)
		}
	}
	if mediaId == "" && mediaKey == "" {
		return nil, Report{}, fmt.Errorf("%w: %s or %s is required", ErrMediaPatchValidation, mediaIdField, mediaKeyField)
	}
	delete(raw, mediaIdField)
	delete(raw, mediaKeyField)

	if len(raw) == 0 {
		return nil, Report{}, fmt.Errorf("%w: at least one field to patch is required", ErrMediaPatchValidation)
	}
	if len(raw) > maxMediaPatchFields {
		return nil, Report{}, fmt.Errorf("%w: %d fields exceed limit of %d", ErrMediaPatchValidation, len(raw), maxMediaPatchFields)
	}

	set := make(map[string]any, len(raw))
	for key, valRaw := range raw {
		path, allowed := mediaPatchableFields[key]
		if !allowed {
			return nil, Report{}, fmt.Errorf("%w: field %q is not patchable", ErrMediaPatchValidation, key)
		}
		val, err := decodeMediaPatchValue(key, valRaw)
		if err != nil {
			return nil, Report{}, fmt.Errorf("%w: field %q %v", ErrMediaPatchValidation, key, err)
		}
		set[path] = val
	}

	// The report's RunId identifies the patched target in the ingest log: prefer the
	// _id, fall back to the recording key.
	runId := mediaId
	if runId == "" {
		runId = mediaKey
	}
	run := MediaPatch{MediaId: mediaId, MediaKey: mediaKey, Set: set}
	report := Report{RunId: runId, Detail: MediaPatchDetail{Fields: len(set)}}
	return run, report, nil
}

// optionalStringField reads a reserved string key (a media identifier) from the
// decoded block body, trimming surrounding space. A missing key yields "" with no
// error; a present but non-string value is a non-retryable validation failure.
func optionalStringField(raw map[string]json.RawMessage, name string) (string, error) {
	valRaw, ok := raw[name]
	if !ok {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(valRaw, &s); err != nil {
		return "", fmt.Errorf("%w: %s must be a string", ErrMediaPatchValidation, name)
	}
	return strings.TrimSpace(s), nil
}

// decodeMediaPatchValue enforces the public wire contract before a value reaches
// Mongo. Field-name validation alone is insufficient: MongoDB accepts arbitrary
// BSON shapes, so a string in star or an object in tagNames would otherwise
// corrupt later typed reads and filters. null is rejected for every field;
// callers clear description with "" and arrays with [].
func decodeMediaPatchValue(key string, raw json.RawMessage) (any, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, errors.New("must not be null")
	}

	switch key {
	case "description":
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, errors.New("must be a string")
		}
		return value, nil
	case "star":
		var value bool
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, errors.New("must be a boolean")
		}
		return value, nil
	case "tagNames", "eventNames", "markerNames":
		var value []string
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, errors.New("must be an array of strings")
		}
		return value, nil
	default:
		return nil, errors.New("is not patchable")
	}
}

// --- Action ----------------------------------------------------------------

// PatchMedia is the media-patch kind's mandatory persistence action: it applies
// the decoded field set to the target media document through the MediaPatcher,
// scoped to the target's organisation. It runs for every source.
type PatchMedia struct{}

// Name implements Action.
func (PatchMedia) Name() string { return "patch_media" }

// RunFor implements Action: the update is mandatory, so it runs for every source.
func (PatchMedia) RunFor(Source) bool { return true }

// Apply implements Action: it applies the decoded patch. The MediaPatcher must be
// wired by any transport that routes the media-patch kind.
func (PatchMedia) Apply(ctx context.Context, scope Scope, target Target, run any) error {
	p, ok := run.(MediaPatch)
	if !ok {
		return fmt.Errorf("ingest: patch expected MediaPatch, got %T", run)
	}
	if scope.Media == nil {
		return fmt.Errorf("%w: no MediaPatcher configured on scope", ErrPermanent)
	}
	return scope.Media.PatchMedia(ctx, target.OrganisationId, p.MediaId, p.MediaKey, p.Set)
}
