package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/uug-ai/models/pkg/models"
)

// --- fakes -----------------------------------------------------------------

type fakeStore struct {
	runs []models.DetectionRun
	err  error
}

func (f *fakeStore) UpsertDetectionRun(_ context.Context, run models.DetectionRun) error {
	if f.err != nil {
		return f.err
	}
	f.runs = append(f.runs, run)
	return nil
}

type fakePromoter struct {
	calls int
	key   string
	runId string
	track []models.FaceRedactionTrack
}

func (f *fakePromoter) PromoteTracks(_ context.Context, key, runId string, tracks []models.FaceRedactionTrack) error {
	f.calls++
	f.key, f.runId, f.track = key, runId, tracks
	return nil
}

func newScope(src Source) (Scope, *fakeStore, *fakePromoter) {
	store := &fakeStore{}
	promoter := &fakePromoter{}
	return Scope{Source: src, Detections: store, Regions: promoter}, store, promoter
}

func target() Target {
	return Target{Key: "cam-1_1700000000_rec", OrganisationId: "org-9", DeviceId: "dev-3", RecordingTimestamp: 1700000000}
}

func approx(got, want float64) bool {
	const eps = 1e-9
	d := got - want
	return d < eps && d > -eps
}

// pixelPayload builds a one-track, one-box pixel-space xyxy run.
func pixelPayload(t *testing.T) json.RawMessage {
	t.Helper()
	body := map[string]any{
		"schemaVersion":   "1.0",
		"source":          map[string]any{"kind": "model", "name": "acme", "version": "1", "runId": "RUN-1"},
		"coordinateSpace": "pixel",
		"media":           map[string]any{"width": 1000, "height": 500},
		"tracks": []any{
			map[string]any{"id": "trk", "boxes": []any{
				map[string]any{"frame": 0, "x1": 100, "y1": 50, "x2": 300, "y2": 250},
			}},
		},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return raw
}

// --- tests -----------------------------------------------------------------

func TestIngest_PixelNormalizesAndStores(t *testing.T) {
	scope, store, _ := newScope(SourcePipeline)

	report, err := Ingest(context.Background(), scope, target(), "detection", pixelPayload(t))
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	if len(store.runs) != 1 {
		t.Fatalf("want 1 stored run, got %d", len(store.runs))
	}
	run := store.runs[0]

	// Identity is stamped from the target, not the payload.
	if run.Key != "cam-1_1700000000_rec" || run.OrganisationId != "org-9" || run.DeviceId != "dev-3" {
		t.Errorf("target identity not stamped: %+v", run)
	}
	if run.RecordingTimestamp != 1700000000 {
		t.Errorf("recordingTimestamp = %d, want 1700000000", run.RecordingTimestamp)
	}
	if run.Task != models.DetectionTask {
		t.Errorf("task = %q, want %q", run.Task, models.DetectionTask)
	}
	if run.OriginalCoordinateSpace != "pixel" || run.OriginalBoxForm != "xyxy" {
		t.Errorf("original space/form = %q/%q", run.OriginalCoordinateSpace, run.OriginalBoxForm)
	}

	// Box normalised to [0,1]: 100/1000, 50/500, 300/1000, 250/500.
	box := run.Tracks[0].FrameCoordinates[0]
	if box.X1 != 0.1 || box.Y1 != 0.1 || box.X2 != 0.3 || box.Y2 != 0.5 {
		t.Errorf("normalised box = %+v, want {0.1,0.1,0.3,0.5}", box)
	}

	detail := report.Detail.(DetectionDetail)
	if detail.TracksStored != 1 || detail.BoxesStored != 1 || report.RunId != "RUN-1" {
		t.Errorf("report = %+v detail = %+v", report, detail)
	}
}

func TestIngest_XYWHFormDetected(t *testing.T) {
	scope, store, _ := newScope(SourceAPI)
	body := map[string]any{
		"source":          map[string]any{"runId": "RUN-2"},
		"coordinateSpace": "normalized",
		"tracks": []any{
			map[string]any{"id": "trk", "boxes": []any{
				map[string]any{"frame": 0, "x": 0.1, "y": 0.2, "w": 0.2, "h": 0.1},
			}},
		},
	}
	raw, _ := json.Marshal(body)

	if _, err := Ingest(context.Background(), scope, target(), "detection", raw); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	run := store.runs[0]
	if run.OriginalBoxForm != "xywh" {
		t.Errorf("box form = %q, want xywh", run.OriginalBoxForm)
	}
	// {x,y,w,h} → xyxy: x2 = 0.1+0.2 = 0.3, y2 = 0.2+0.1 = 0.3.
	box := run.Tracks[0].FrameCoordinates[0]
	if !approx(box.X2, 0.3) || !approx(box.Y2, 0.3) {
		t.Errorf("converted box = %+v, want x2=0.3 y2=0.3", box)
	}
}

func TestIngest_PromoteGatedBySource(t *testing.T) {
	t.Run("pipeline promotes", func(t *testing.T) {
		scope, _, promoter := newScope(SourcePipeline)
		if _, err := Ingest(context.Background(), scope, target(), "detection", pixelPayload(t)); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		if promoter.calls != 1 {
			t.Errorf("promoter calls = %d, want 1", promoter.calls)
		}
		if promoter.runId != "RUN-1" || promoter.key != "cam-1_1700000000_rec" {
			t.Errorf("promote args key=%q runId=%q", promoter.key, promoter.runId)
		}
	})

	t.Run("api does not promote", func(t *testing.T) {
		scope, store, promoter := newScope(SourceAPI)
		if _, err := Ingest(context.Background(), scope, target(), "detection", pixelPayload(t)); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		// Core write still happens for the API transport...
		if len(store.runs) != 1 {
			t.Fatalf("want 1 stored run, got %d", len(store.runs))
		}
		// ...but the trusted-only side-effect is skipped.
		if promoter.calls != 0 {
			t.Errorf("promoter calls = %d, want 0 for API source", promoter.calls)
		}
	})
}

func TestIngest_OutOfFrameBoxRejected(t *testing.T) {
	scope, store, _ := newScope(SourcePipeline)
	body := map[string]any{
		"source":          map[string]any{"runId": "RUN-3"},
		"coordinateSpace": "normalized",
		"tracks": []any{
			map[string]any{"id": "trk", "boxes": []any{
				map[string]any{"frame": 0, "x1": 0.1, "y1": 0.1, "x2": 0.3, "y2": 0.4},
				map[string]any{"frame": 1, "x1": 1.2, "y1": 1.2, "x2": 1.5, "y2": 1.6},
			}},
		},
	}
	raw, _ := json.Marshal(body)

	report, err := Ingest(context.Background(), scope, target(), "detection", raw)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	detail := report.Detail.(DetectionDetail)
	if detail.BoxesStored != 1 {
		t.Errorf("boxesStored = %d, want 1", detail.BoxesStored)
	}
	if len(detail.Rejected) != 1 || detail.Rejected[0].Reason != "box_out_of_frame" || detail.Rejected[0].Frame != 1 {
		t.Errorf("rejected = %+v, want one box_out_of_frame at frame 1", detail.Rejected)
	}
	if got := len(store.runs[0].Tracks[0].FrameCoordinates); got != 1 {
		t.Errorf("stored frame coords = %d, want 1", got)
	}
}

func TestIngest_AllBoxesRejected(t *testing.T) {
	scope, store, _ := newScope(SourcePipeline)
	body := map[string]any{
		"source":          map[string]any{"runId": "RUN-5"},
		"coordinateSpace": "normalized",
		"tracks": []any{
			map[string]any{"id": "trk", "boxes": []any{
				map[string]any{"frame": 0, "x1": 1.2, "y1": 1.2, "x2": 1.5, "y2": 1.6},
			}},
		},
	}
	raw, _ := json.Marshal(body)

	_, err := Ingest(context.Background(), scope, target(), "detection", raw)
	if !errors.Is(err, ErrAllBoxesRejected) {
		t.Errorf("err = %v, want ErrAllBoxesRejected", err)
	}
	if len(store.runs) != 0 {
		t.Errorf("nothing should be stored when all boxes rejected, got %d runs", len(store.runs))
	}
}

func TestIngest_UnknownKind(t *testing.T) {
	scope, _, _ := newScope(SourceAPI)
	_, err := Ingest(context.Background(), scope, target(), "nosuchkind", json.RawMessage(`{}`))
	if !errors.Is(err, ErrUnknownKind) {
		t.Errorf("err = %v, want ErrUnknownKind", err)
	}
}

func TestIngest_PoseUsesDetectionContract(t *testing.T) {
	scope, store, _ := newScope(SourceAPI)
	body := map[string]any{
		"task":            "pose",
		"source":          map[string]any{"runId": "RUN-POSE"},
		"coordinateSpace": "normalized",
		"tracks":          []any{map[string]any{"id": "t", "boxes": []any{map[string]any{"frame": 0, "x": 0.1, "y": 0.1, "w": 0.1, "h": 0.1}}}},
	}
	raw, _ := json.Marshal(body)
	report, err := Ingest(context.Background(), scope, target(), "detection", raw)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if len(store.runs) != 1 {
		t.Fatalf("want 1 stored run, got %d", len(store.runs))
	}
	if store.runs[0].Task != "pose" {
		t.Errorf("run.Task = %q, want \"pose\"", store.runs[0].Task)
	}
	if detail := report.Detail.(DetectionDetail); report.RunId != "RUN-POSE" || detail.BoxesStored != 1 {
		t.Errorf("report = %+v detail = %+v", report, detail)
	}
}

func TestIngest_UnknownTask(t *testing.T) {
	scope, _, _ := newScope(SourceAPI)
	body := map[string]any{
		"task":            "segmentation",
		"source":          map[string]any{"runId": "RUN-4"},
		"coordinateSpace": "normalized",
		"tracks":          []any{map[string]any{"id": "t", "boxes": []any{map[string]any{"frame": 0, "x": 0.1, "y": 0.1, "w": 0.1, "h": 0.1}}}},
	}
	raw, _ := json.Marshal(body)
	_, err := Ingest(context.Background(), scope, target(), "detection", raw)
	if !errors.Is(err, ErrUnknownTask) {
		t.Errorf("err = %v, want ErrUnknownTask", err)
	}
}

func TestIngest_ValidationErrors(t *testing.T) {
	scope, _, _ := newScope(SourceAPI)

	cases := map[string]map[string]any{
		"missing runId": {
			"source":          map[string]any{},
			"coordinateSpace": "normalized",
			"tracks":          []any{map[string]any{"id": "t", "boxes": []any{map[string]any{"frame": 0, "x": 0.1, "y": 0.1, "w": 0.1, "h": 0.1}}}},
		},
		"pixel without media": {
			"source":          map[string]any{"runId": "R"},
			"coordinateSpace": "pixel",
			"tracks":          []any{map[string]any{"id": "t", "boxes": []any{map[string]any{"frame": 0, "x1": 1, "y1": 1, "x2": 2, "y2": 2}}}},
		},
		"track without boxes": {
			"source":          map[string]any{"runId": "R"},
			"coordinateSpace": "normalized",
			"tracks":          []any{map[string]any{"id": "t", "boxes": []any{}}},
		},
		"bad coordinate space": {
			"source":          map[string]any{"runId": "R"},
			"coordinateSpace": "weird",
			"tracks":          []any{map[string]any{"id": "t", "boxes": []any{map[string]any{"frame": 0, "x": 0.1, "y": 0.1, "w": 0.1, "h": 0.1}}}},
		},
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(body)
			if _, err := Ingest(context.Background(), scope, target(), "detection", raw); err == nil {
				t.Errorf("expected validation error for %q", name)
			}
		})
	}
}
