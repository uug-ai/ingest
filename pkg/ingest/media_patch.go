package ingest

import (
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
// no-op, not an error.
type MediaPatcher interface {
	// PatchMedia applies fields ($set) to the media document identified by
	// mediaId within organisationId. fields is keyed by the media document's own
	// (dot-notation) field paths, already validated against the patchable allow
	// list by the decode step.
	PatchMedia(ctx context.Context, organisationId, mediaId string, fields map[string]any) error
}

// ErrMediaPatchValidation tags a non-retryable media-patch validation failure (a
// missing/invalid media id, an unknown field, an empty patch): redelivery cannot
// fix it, so the caller drops the block rather than re-queuing it.
var ErrMediaPatchValidation = errors.New("ingest: invalid media-patch block")

// mediaIdField is the reserved key that names the target media document; every
// other key in the block is a field to patch.
const mediaIdField = "mediaId"

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

// MediaPatch is the media-patch kind's typed run: the target media id and the
// already-validated, org-scoped $set field map (keyed by media document paths).
type MediaPatch struct {
	MediaId string
	Set     map[string]any
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
func decodeMediaPatch(_ Scope, _ Target, payload json.RawMessage) (any, Report, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, Report{}, fmt.Errorf("ingest: decode media-patch payload: %w", err)
	}

	idRaw, ok := raw[mediaIdField]
	if !ok {
		return nil, Report{}, fmt.Errorf("%w: %s is required", ErrMediaPatchValidation, mediaIdField)
	}
	var mediaId string
	if err := json.Unmarshal(idRaw, &mediaId); err != nil {
		return nil, Report{}, fmt.Errorf("%w: %s must be a string", ErrMediaPatchValidation, mediaIdField)
	}
	mediaId = strings.TrimSpace(mediaId)
	if mediaId == "" {
		return nil, Report{}, fmt.Errorf("%w: %s is required", ErrMediaPatchValidation, mediaIdField)
	}
	// The media id names a document by its _id; reject a malformed one here (a
	// non-retryable validation failure) so the sink never has to.
	if _, err := primitive.ObjectIDFromHex(mediaId); err != nil {
		return nil, Report{}, fmt.Errorf("%w: %s %q is not a valid id", ErrMediaPatchValidation, mediaIdField, mediaId)
	}
	delete(raw, mediaIdField)

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
		var val any
		if err := json.Unmarshal(valRaw, &val); err != nil {
			return nil, Report{}, fmt.Errorf("%w: field %q has an invalid value: %v", ErrMediaPatchValidation, key, err)
		}
		set[path] = val
	}

	run := MediaPatch{MediaId: mediaId, Set: set}
	report := Report{RunId: mediaId, Detail: MediaPatchDetail{Fields: len(set)}}
	return run, report, nil
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
		return errors.New("ingest: no MediaPatcher configured on scope")
	}
	return scope.Media.PatchMedia(ctx, target.OrganisationId, p.MediaId, p.Set)
}
