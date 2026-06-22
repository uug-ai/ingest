package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/uug-ai/models/pkg/api"
	"github.com/uug-ai/models/pkg/models"
)

// Validation and normalisation limits for the detection box task. They mirror
// the documented detection contract so the general /ingest door enforces the
// same envelope as the typed /detections endpoint.
const (
	// boundsTolerance allows float rounding when checking a normalized box
	// stays within the [0, 1] frame.
	boundsTolerance = 0.01

	// Schema version the package currently implements. A major mismatch is
	// rejected; a minor mismatch is accepted with a warning. An empty
	// schemaVersion is allowed — the field is optional on this door.
	supportedSchemaMajor = 1
	currentSchemaMinor   = 0

	maxTracksPerRun  = 5000
	maxBoxesPerTrack = 100000
	maxNameLen       = 64
	maxVersionLen    = 32
	maxRunIdLen      = 40
	maxMetaBytes     = 4096
)

// Per-box rejection reasons surfaced in the report's Rejected list.
const (
	reasonBoxOutOfFrame  = "box_out_of_frame"
	reasonMissingGeom    = "box_missing_geometry"
	reasonNonPositiveDim = "box_non_positive_dimensions"
	reasonMissingScale   = "box_missing_media_scale"
)

// Box geometry forms recorded in DetectionRun.OriginalBoxForm.
const (
	boxFormXYWH  = "xywh"
	boxFormXYXY  = "xyxy"
	boxFormMixed = "mixed"
)

// --- Sinks (implemented by each app's repository; kept as interfaces so this
// package stays infra-free) -------------------------------------------------

// DetectionStore is the mandatory sink for a detection run. The implementation
// MUST upsert by (run.Key, run.Source.RunId) so a redelivered or re-posted run
// replaces rather than duplicates.
type DetectionStore interface {
	UpsertDetectionRun(ctx context.Context, run models.DetectionRun) error
}

// RegionPromoter is the optional sink that copies a run's tracks onto the
// recording's redaction regions. The implementation MUST replace a run's prior
// contribution keyed by runId rather than append, so a replay stays idempotent.
type RegionPromoter interface {
	PromoteTracks(ctx context.Context, key, runId string, tracks []models.FaceRedactionTrack) error
}

// --- Kind handler ----------------------------------------------------------

// detectionHandler is the reference kind. Its sequence is two actions:
// (1) the mandatory UpsertDetectionRun, (2) the trusted-only
// PromoteTracksToRegions — exactly the "insert the run AND adjust the
// region-selection" pair the model types are shaped for.
var detectionHandler = Handler{
	Kind:           KindDetection,
	AllowedSources: []Source{SourceAPI, SourcePipeline},
	Decode:         decodeDetection,
	Actions:        []Action{UpsertDetectionRun{}, PromoteTracksToRegions{}},
}

// DetectionDetail is the detection kind's ReportDetail: how many tracks and
// boxes were stored and which boxes were rejected during normalisation.
type DetectionDetail struct {
	TracksStored int
	BoxesStored  int
	Rejected     []api.DetectionRejection
}

// Summary implements ReportDetail.
func (d DetectionDetail) Summary() string {
	return fmt.Sprintf("%d track(s), %d box(es), %d rejected", d.TracksStored, d.BoxesStored, len(d.Rejected))
}

// decodeDetection unmarshals the envelope payload into the shared
// api.PostDetectionsRequest (the exact same type the HTTP door binds), routes
// by Task to the matching validator/normaliser, and stamps the target-derived
// identity onto the run. It runs once per request; the actions consume its
// typed output.
func decodeDetection(scope Scope, target Target, payload json.RawMessage) (any, Report, error) {
	var req api.PostDetectionsRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, Report{}, fmt.Errorf("ingest: decode detection payload: %w", err)
	}

	task := req.Task
	if task == "" {
		task = models.DetectionTask
	}

	th, ok := taskRegistry[task]
	if !ok {
		return nil, Report{}, fmt.Errorf("%w: detection task %q", ErrUnknownTask, task)
	}

	if err := th.Validate(req); err != nil {
		return nil, Report{}, err
	}

	run, report := th.Normalize(req)

	// A run in which every box was rejected stores nothing — surface it so the
	// adapter can reject the whole request rather than persist an empty run.
	detail, _ := report.Detail.(DetectionDetail)
	if detail.BoxesStored == 0 {
		report.RunId = req.Source.RunId
		return nil, report, ErrAllBoxesRejected
	}

	// Stamp the resolved recording identity (the producer never sends these;
	// the adapter resolved them from mediaKey/analysisId or the event fileName).
	run.Key = target.Key
	run.OrganisationId = target.OrganisationId
	run.DeviceId = target.DeviceId
	run.RecordingTimestamp = target.RecordingTimestamp
	if run.Task == "" {
		run.Task = task
	}
	report.RunId = run.Source.RunId

	return run, report, nil
}

// ErrUnknownTask is returned when a detection payload names a task with no
// registered handler (e.g. "pose" before it ships).
var ErrUnknownTask = errors.New("ingest: no handler registered for task")

// ErrAllBoxesRejected is returned when validation passed but every box in the
// run failed normalisation, so there is nothing to store. The adapter maps it
// to a 400 (the run's Rejected list explains why).
var ErrAllBoxesRejected = errors.New("ingest: all boxes were rejected")

// ErrUnsupportedSchema is returned when a non-empty schemaVersion names a major
// version this package does not implement. The adapter maps it to a 400.
var ErrUnsupportedSchema = errors.New("ingest: unsupported schemaVersion")

// ErrValidation wraps a request-envelope validation failure (bad coordinate
// space, missing media scale, oversize/duplicate tracks, etc.). The adapter
// maps it to a 400; the wrapped message carries the detail.
var ErrValidation = errors.New("ingest: invalid detection request")

// --- Task sub-registry (inside the detection kind) -------------------------

// TaskHandler selects the validation / normalisation for one detection task.
// All tasks produce a models.DetectionRun and share the detections collection;
// only the geometry contract differs.
type TaskHandler interface {
	Validate(req api.PostDetectionsRequest) error
	Normalize(req api.PostDetectionsRequest) (models.DetectionRun, Report)
}

// taskRegistry maps a detection task to its handler. Every task today shares
// the detection bounding-box contract (api.PostDetectionsRequest), so they all
// route to boxTask — pose, like box, reports a person/keypoint subject as an
// axis-aligned box per frame and is stored as a DetectionRun. A task that later
// needs a different geometry contract gets its own handler here without
// touching either transport.
var taskRegistry = map[string]TaskHandler{
	models.DetectionTask: boxTask{}, // "detection" (default / box)
	"pose":               boxTask{}, // pose producer emits the detection contract
}

// --- box task --------------------------------------------------------------

// boxTask is the axis-aligned bounding-box task: today's detection contract.
// It accepts both {x,y,w,h} and {x1,y1,x2,y2} box forms and both pixel and
// normalized coordinate spaces, and stores the run normalised to [0,1] xyxy.
// Its validate/normalise mirror the typed /detections endpoint so the general
// door enforces the same envelope, limits and warnings.
type boxTask struct{}

func (boxTask) Validate(req api.PostDetectionsRequest) error {
	if req.Source.RunId == "" {
		return fmt.Errorf("%w: source.runId is required", ErrValidation)
	}
	if req.SchemaVersion != "" {
		if _, supported := checkSchemaVersion(req.SchemaVersion); !supported {
			return fmt.Errorf("%w: %s", ErrUnsupportedSchema, req.SchemaVersion)
		}
	}
	if req.CoordinateSpace != "pixel" && req.CoordinateSpace != "normalized" {
		return fmt.Errorf("%w: coordinateSpace must be \"pixel\" or \"normalized\", got %q", ErrValidation, req.CoordinateSpace)
	}
	if req.CoordinateSpace == "pixel" && (req.Media.Width <= 0 || req.Media.Height <= 0) {
		return fmt.Errorf("%w: media.width and media.height are required when coordinateSpace is \"pixel\"", ErrValidation)
	}
	if len(req.Tracks) == 0 {
		return fmt.Errorf("%w: at least one track is required", ErrValidation)
	}
	if len(req.Tracks) > maxTracksPerRun {
		return fmt.Errorf("%w: too many tracks: %d exceeds limit of %d", ErrValidation, len(req.Tracks), maxTracksPerRun)
	}
	if len(req.Source.Name) > maxNameLen {
		return fmt.Errorf("%w: source.name exceeds %d characters", ErrValidation, maxNameLen)
	}
	if len(req.Source.Version) > maxVersionLen {
		return fmt.Errorf("%w: source.version exceeds %d characters", ErrValidation, maxVersionLen)
	}
	if len(req.Source.RunId) > maxRunIdLen {
		return fmt.Errorf("%w: source.runId exceeds %d characters", ErrValidation, maxRunIdLen)
	}
	if len(req.Task) > maxNameLen {
		return fmt.Errorf("%w: task exceeds %d characters", ErrValidation, maxNameLen)
	}
	trackIds := make(map[string]struct{}, len(req.Tracks))
	for i, t := range req.Tracks {
		id := t.Id.String()
		if id == "" {
			return fmt.Errorf("%w: track %d is missing an id", ErrValidation, i)
		}
		if len(id) > maxNameLen {
			return fmt.Errorf("%w: track %d id exceeds %d characters", ErrValidation, i, maxNameLen)
		}
		if _, seen := trackIds[id]; seen {
			return fmt.Errorf("%w: duplicate track id %q", ErrValidation, id)
		}
		trackIds[id] = struct{}{}
		if len(t.Boxes) == 0 {
			return fmt.Errorf("%w: track %d has no boxes", ErrValidation, i)
		}
		if len(t.Boxes) > maxBoxesPerTrack {
			return fmt.Errorf("%w: track %d has too many boxes: %d exceeds limit of %d", ErrValidation, i, len(t.Boxes), maxBoxesPerTrack)
		}
		if msg := validateMetaSize(fmt.Sprintf("track %d meta", i), t.Meta); msg != "" {
			return fmt.Errorf("%w: %s", ErrValidation, msg)
		}
		for j, b := range t.Boxes {
			if msg := validateMetaSize(fmt.Sprintf("track %d box %d meta", i, j), b.Meta); msg != "" {
				return fmt.Errorf("%w: %s", ErrValidation, msg)
			}
		}
	}
	return nil
}

// Normalize converts the wire request into the stored DetectionRun. Per-box
// validation is non-fatal: an invalid box is collected in report.Rejected and
// skipped, a track with no surviving box is dropped, and consistency issues
// (timestamp/frame drift, out-of-range frames, duplicate frames, schema minor
// mismatch) become warnings. When every box is rejected the run is left empty
// and report.BoxesStored stays zero so decodeDetection raises
// ErrAllBoxesRejected.
func (boxTask) Normalize(req api.PostDetectionsRequest) (models.DetectionRun, Report) {
	detail := DetectionDetail{Rejected: []api.DetectionRejection{}}
	warnings := []string{}

	normalized := req.CoordinateSpace != "pixel"
	scaleX, scaleY := 1.0, 1.0
	if !normalized {
		scaleX = float64(req.Media.Width)
		scaleY = float64(req.Media.Height)
	}

	totalBoxes := 0
	sawXYWH, sawXYXY := false, false
	timestampMismatches := 0
	frameOutOfRange := 0
	duplicateFrames := 0
	storedTracks := make([]models.FaceRedactionTrack, 0, len(req.Tracks))

	for _, t := range req.Tracks {
		trackId := t.Id.String()
		frameCoords := make(map[int64]models.TrackBox, len(t.Boxes))
		frames := make([]int64, 0, len(t.Boxes))

		for _, b := range t.Boxes {
			totalBoxes++

			switch boxForm(b) {
			case boxFormXYWH:
				sawXYWH = true
			case boxFormXYXY:
				sawXYXY = true
			}

			// Non-fatal consistency checks surfaced as warnings.
			if req.Media.Fps > 0 && b.TimestampMs > 0 {
				expected := float64(b.Frame) * 1000.0 / req.Media.Fps
				if math.Abs(float64(b.TimestampMs)-expected) > 1000.0/req.Media.Fps {
					timestampMismatches++
				}
			}
			if req.Media.FrameCount > 0 && b.Frame >= req.Media.FrameCount {
				frameOutOfRange++
			}

			box, reason := normalizeDetectionBox(b, normalized, scaleX, scaleY)
			if reason != "" {
				detail.Rejected = append(detail.Rejected, api.DetectionRejection{
					TrackId: trackId,
					Frame:   b.Frame,
					Reason:  reason,
				})
				continue
			}

			box.Edited = b.Edited
			box.Smoothed = b.Smoothed
			// Preserve the producer's per-box model output for later
			// re-thresholding/auditing (the geometry alone is otherwise lossy).
			box.Confidence = b.Confidence
			box.ClassId = b.ClassId
			box.Label = b.Label
			if _, dup := frameCoords[b.Frame]; dup {
				// A track must not carry two boxes for the same frame; the
				// later one overwrites the earlier. Surface this so producers
				// notice silent loss instead of guessing from the count.
				duplicateFrames++
			} else {
				frames = append(frames, b.Frame)
				detail.BoxesStored++
			}
			// Last write wins for a repeated frame within the same track.
			frameCoords[b.Frame] = box
		}

		if len(frameCoords) == 0 {
			// Every box in this track was rejected; drop the track.
			continue
		}

		sort.Slice(frames, func(i, j int) bool { return frames[i] < frames[j] })

		storedTracks = append(storedTracks, models.FaceRedactionTrack{
			Id:               trackId,
			Classified:       t.Label,
			Frames:           frames,
			ColorString:      colorSlice(t.Color),
			Selected:         false,
			DeletedFrames:    t.DeletedFrames,
			FrameCoordinates: frameCoords,
			Confidence:       t.Confidence,
			ClassId:          t.ClassId,
			Shape:            t.Shape,
		})
		detail.TracksStored++
	}

	if timestampMismatches > 0 {
		warnings = append(warnings, fmt.Sprintf("TIMESTAMP_FRAME_MISMATCH: %d box(es)", timestampMismatches))
	}
	if frameOutOfRange > 0 {
		warnings = append(warnings, fmt.Sprintf("FRAME_OUT_OF_RANGE: %d box(es)", frameOutOfRange))
	}
	if duplicateFrames > 0 {
		warnings = append(warnings, fmt.Sprintf("DUPLICATE_FRAME: %d box(es) overwritten", duplicateFrames))
	}
	if warning, _ := checkSchemaVersion(req.SchemaVersion); warning != "" {
		warnings = append(warnings, warning)
	}

	report := Report{Warnings: warnings, Detail: detail}

	if totalBoxes > 0 && detail.BoxesStored == 0 {
		// Nothing survived; the empty report drives ErrAllBoxesRejected upstream.
		return models.DetectionRun{}, report
	}

	source := req.Source
	if source.RotationApplied == nil {
		// Documented default: boxes are against the oriented frame.
		defaultRotation := true
		source.RotationApplied = &defaultRotation
	}

	run := models.DetectionRun{
		Source:                  source,
		SchemaVersion:           req.SchemaVersion,
		Media:                   req.Media,
		Categories:              req.Categories,
		Tracks:                  storedTracks,
		OriginalCoordinateSpace: req.CoordinateSpace,
		OriginalBoxForm:         boxFormLabel(sawXYWH, sawXYXY),
		CreatedAt:               time.Now().UnixMilli(),
	}
	return run, report
}

// boxForm reports which geometry form a wire box uses, or "" when neither is
// fully present.
func boxForm(b api.DetectionBoxInput) string {
	if b.X != nil && b.Y != nil && b.W != nil && b.H != nil {
		return boxFormXYWH
	}
	if b.X1 != nil && b.Y1 != nil && b.X2 != nil && b.Y2 != nil {
		return boxFormXYXY
	}
	return ""
}

// boxFormLabel collapses the observed forms into the run-level OriginalBoxForm.
func boxFormLabel(sawXYWH, sawXYXY bool) string {
	switch {
	case sawXYWH && sawXYXY:
		return boxFormMixed
	case sawXYWH:
		return boxFormXYWH
	case sawXYXY:
		return boxFormXYXY
	default:
		return ""
	}
}

// normalizeDetectionBox converts a single wire box into a normalized TrackBox.
// It returns a rejection reason string when the box cannot be stored. Pixel
// boxes are divided by the media scale first; boxes within boundsTolerance of
// the frame are clamped, those further out are rejected.
func normalizeDetectionBox(b api.DetectionBoxInput, alreadyNormalized bool, scaleX, scaleY float64) (models.TrackBox, string) {
	var x1, y1, x2, y2 float64

	switch {
	case b.X != nil && b.Y != nil && b.W != nil && b.H != nil:
		// Preferred {x, y, w, h} top-left + size form.
		if *b.W <= 0 || *b.H <= 0 {
			return models.TrackBox{}, reasonNonPositiveDim
		}
		x1, y1 = *b.X, *b.Y
		x2, y2 = *b.X+*b.W, *b.Y+*b.H
	case b.X1 != nil && b.Y1 != nil && b.X2 != nil && b.Y2 != nil:
		// Legacy {x1, y1, x2, y2} form.
		x1, y1, x2, y2 = *b.X1, *b.Y1, *b.X2, *b.Y2
		if x2 <= x1 || y2 <= y1 {
			return models.TrackBox{}, reasonNonPositiveDim
		}
	default:
		return models.TrackBox{}, reasonMissingGeom
	}

	if !alreadyNormalized {
		if scaleX <= 0 || scaleY <= 0 {
			return models.TrackBox{}, reasonMissingScale
		}
		x1, x2 = x1/scaleX, x2/scaleX
		y1, y2 = y1/scaleY, y2/scaleY
	}

	if !withinFrame(x1, y1, x2, y2) {
		return models.TrackBox{}, reasonBoxOutOfFrame
	}

	// Within tolerance but slightly outside [0, 1]: clamp so downstream
	// consumers never have to defend against out-of-frame coordinates.
	x1, y1 = clampUnit(x1), clampUnit(y1)
	x2, y2 = clampUnit(x2), clampUnit(y2)

	return models.TrackBox{X1: x1, Y1: y1, X2: x2, Y2: y2}, ""
}

func withinFrame(x1, y1, x2, y2 float64) bool {
	return x1 >= -boundsTolerance &&
		y1 >= -boundsTolerance &&
		x2 <= 1+boundsTolerance &&
		y2 <= 1+boundsTolerance
}

// clampUnit constrains a normalized coordinate to the [0, 1] frame.
func clampUnit(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func colorSlice(color string) []string {
	if color == "" {
		return nil
	}
	return []string{color}
}

// validateMetaSize enforces the documented 4KB cap on per-object meta payloads.
func validateMetaSize(label string, meta map[string]interface{}) string {
	if len(meta) == 0 {
		return ""
	}
	encoded, err := json.Marshal(meta)
	if err != nil {
		return fmt.Sprintf("%s could not be encoded: %v", label, err)
	}
	if len(encoded) > maxMetaBytes {
		return fmt.Sprintf("%s exceeds %d bytes", label, maxMetaBytes)
	}
	return ""
}

// checkSchemaVersion validates a schemaVersion against what the package
// implements. A major mismatch (or unparseable value) is unsupported; a minor
// mismatch is accepted but reported as a warning.
func checkSchemaVersion(version string) (warning string, supported bool) {
	major, minor, ok := parseSchemaVersion(version)
	if !ok || major != supportedSchemaMajor {
		return "", false
	}
	if minor != currentSchemaMinor {
		return fmt.Sprintf("SCHEMA_VERSION_MINOR_MISMATCH: received %s, server implements %d.%d", version, supportedSchemaMajor, currentSchemaMinor), true
	}
	return "", true
}

func parseSchemaVersion(version string) (major, minor int, ok bool) {
	parts := strings.SplitN(strings.TrimSpace(version), ".", 2)
	if len(parts) == 0 || parts[0] == "" {
		return 0, 0, false
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, false
	}
	if len(parts) == 2 && parts[1] != "" {
		minor, err = strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, false
		}
	}
	return major, minor, true
}

// --- Actions ---------------------------------------------------------------

// UpsertDetectionRun is the mandatory first action: it persists the normalised
// run to the detections collection, keyed by (Key, Source.RunId). It runs for
// every transport — an API push and a pipeline stage store the same run.
type UpsertDetectionRun struct{}

func (UpsertDetectionRun) Name() string       { return "upsert_detection_run" }
func (UpsertDetectionRun) RunFor(Source) bool { return true }

func (UpsertDetectionRun) Apply(ctx context.Context, scope Scope, _ Target, run any) error {
	dr, ok := run.(models.DetectionRun)
	if !ok {
		return fmt.Errorf("ingest: upsert expected models.DetectionRun, got %T", run)
	}
	if scope.Detections == nil {
		return errors.New("ingest: no DetectionStore configured on scope")
	}
	return scope.Detections.UpsertDetectionRun(ctx, dr)
}

// PromoteTracksToRegions is the optional second action: it copies the run's
// tracks onto the recording's redaction region-selection. It is gated to the
// trusted pipeline transport — an external API push stores the run but never
// auto-mutates redaction (a policy decision, not just routing).
type PromoteTracksToRegions struct{}

func (PromoteTracksToRegions) Name() string         { return "promote_tracks_to_regions" }
func (PromoteTracksToRegions) RunFor(s Source) bool { return s == SourcePipeline }

func (PromoteTracksToRegions) Apply(ctx context.Context, scope Scope, target Target, run any) error {
	dr, ok := run.(models.DetectionRun)
	if !ok {
		return fmt.Errorf("ingest: promote expected models.DetectionRun, got %T", run)
	}
	if scope.Regions == nil {
		// Promotion is opt-in by wiring: the transport is trusted to promote
		// (RunFor), but a transport that only stores runs (e.g. generic
		// detection/pose ingestion mirroring the detection endpoint) supplies
		// no RegionPromoter. With nothing to promote through, this is a no-op,
		// not a failure.
		return nil
	}
	return scope.Regions.PromoteTracks(ctx, target.Key, dr.Source.RunId, dr.Tracks)
}
