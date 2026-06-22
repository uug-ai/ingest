package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/uug-ai/models/pkg/models"
)

// fakeMarkerStore records upserted markers (and can be made to fail) so the
// marker-kind tests can assert what was persisted without a database.
type fakeMarkerStore struct {
	markers []models.Marker
	err     error
}

func (f *fakeMarkerStore) UpsertMarkers(_ context.Context, markers []models.Marker) error {
	if f.err != nil {
		return f.err
	}
	f.markers = append(f.markers, markers...)
	return nil
}

// markerBlock builds a marker block with the given name/start and an optional
// raw deviceId, so a test can exercise the target-stamping fallback.
func markerBlock(t *testing.T, name string, start, end int64, deviceId string) Block {
	t.Helper()
	body := map[string]any{"name": name, "startTimestamp": start, "endTimestamp": end}
	if deviceId != "" {
		body["deviceId"] = deviceId
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal marker block: %v", err)
	}
	return Block{Type: KindMarker, Data: raw}
}

func TestIngestBlocks_Marker(t *testing.T) {
	store := &fakeMarkerStore{}
	scope := Scope{Source: SourcePipeline, Markers: store}

	batch, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
		Blocks: []Block{markerBlock(t, "  ABC-123 ", 100, 105, "")},
	})
	if err != nil {
		t.Fatalf("IngestBlocks: %v", err)
	}
	if len(store.markers) != 1 {
		t.Fatalf("want 1 stored marker, got %d", len(store.markers))
	}
	got := store.markers[0]
	if got.Name != "ABC-123" {
		t.Errorf("name = %q, want trimmed ABC-123", got.Name)
	}
	if got.OrganisationId != target().OrganisationId {
		t.Errorf("organisationId = %q, want %q stamped from target", got.OrganisationId, target().OrganisationId)
	}
	if got.DeviceId != target().DeviceId {
		t.Errorf("deviceId = %q, want %q stamped from target when wire omits it", got.DeviceId, target().DeviceId)
	}
	if got.Duration != 5 {
		t.Errorf("duration = %d, want 5 (end-start)", got.Duration)
	}
	if len(batch.Blocks) != 1 || batch.Blocks[0].Type != KindMarker {
		t.Fatalf("want 1 marker block report, got %+v", batch.Blocks)
	}
	if d, ok := batch.Blocks[0].Detail.(MarkerDetail); !ok || d.Stored != 1 {
		t.Errorf("detail = %#v, want MarkerDetail{Stored:1}", batch.Blocks[0].Detail)
	}
}

func TestDecodeMarker_KeepsWireDeviceAndCollapsesPointInTime(t *testing.T) {
	store := &fakeMarkerStore{}
	scope := Scope{Source: SourcePipeline, Markers: store}

	// endTimestamp 0 (omitted) is before start: the span collapses to the start.
	if _, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
		Blocks: []Block{markerBlock(t, "point", 200, 0, "dev-explicit")},
	}); err != nil {
		t.Fatalf("IngestBlocks: %v", err)
	}
	got := store.markers[0]
	if got.DeviceId != "dev-explicit" {
		t.Errorf("deviceId = %q, want the wire-supplied dev-explicit", got.DeviceId)
	}
	if got.EndTimestamp != 200 || got.Duration != 0 {
		t.Errorf("end/duration = %d/%d, want 200/0 (collapsed point-in-time)", got.EndTimestamp, got.Duration)
	}
}

func TestIngestBlocks_MarkerRejectsAPISource(t *testing.T) {
	store := &fakeMarkerStore{}
	scope := Scope{Source: SourceAPI, Markers: store}

	_, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
		Blocks: []Block{markerBlock(t, "ABC-123", 100, 105, "")},
	})
	if !errors.Is(err, ErrSourceNotAllowed) {
		t.Fatalf("err = %v, want ErrSourceNotAllowed (marker is pipeline-only)", err)
	}
	if len(store.markers) != 0 {
		t.Errorf("want nothing stored when the source is forbidden, got %d", len(store.markers))
	}
}

func TestDecodeMarker_Validation(t *testing.T) {
	scope := Scope{Source: SourcePipeline, Markers: &fakeMarkerStore{}}
	cases := map[string]map[string]any{
		"missing name":  {"startTimestamp": 100},
		"missing start": {"name": "x"},
	}
	for label, body := range cases {
		t.Run(label, func(t *testing.T) {
			raw, _ := json.Marshal(body)
			_, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
				Blocks: []Block{{Type: KindMarker, Data: raw}},
			})
			if !errors.Is(err, ErrMarkerValidation) {
				t.Fatalf("err = %v, want ErrMarkerValidation", err)
			}
		})
	}
}

func TestUpsertMarker_RequiresStore(t *testing.T) {
	// Markers sink nil: the mandatory action tags the failure ErrPersist.
	scope := Scope{Source: SourcePipeline}
	_, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
		Blocks: []Block{markerBlock(t, "ABC-123", 100, 105, "")},
	})
	if !errors.Is(err, ErrPersist) {
		t.Fatalf("err = %v, want ErrPersist when no MarkerStore is configured", err)
	}
}
