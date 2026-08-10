package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// fakeMediaPatcher records the patches applied (and can be made to fail) so the
// media-patch tests can assert what was written without a database.
type fakeMediaPatcher struct {
	calls []mediaPatchCall
	err   error
}

type mediaPatchCall struct {
	organisationId string
	mediaId        string
	mediaKey       string
	fields         map[string]any
}

func (f *fakeMediaPatcher) PatchMedia(_ context.Context, organisationId, mediaId, mediaKey string, fields map[string]any) error {
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, mediaPatchCall{organisationId: organisationId, mediaId: mediaId, mediaKey: mediaKey, fields: fields})
	return nil
}

// mediaPatchBlock builds a media-patch block from a flat body (mediaId/mediaKey
// plus the fields to set).
func mediaPatchBlock(t *testing.T, body map[string]any) Block {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal media-patch block: %v", err)
	}
	return Block{Type: KindMediaPatch, Data: raw}
}

func TestIngestBlocks_MediaPatch(t *testing.T) {
	store := &fakeMediaPatcher{}
	scope := Scope{Source: SourcePipeline, Media: store}

	id := primitive.NewObjectID().Hex()
	batch, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
		Blocks: []Block{mediaPatchBlock(t, map[string]any{
			"mediaId":     id,
			"description": "a new description",
			"star":        true,
		})},
	})
	if err != nil {
		t.Fatalf("IngestBlocks: %v", err)
	}
	if len(store.calls) != 1 {
		t.Fatalf("want 1 patch call, got %d", len(store.calls))
	}
	call := store.calls[0]
	if call.mediaId != id {
		t.Errorf("mediaId = %q, want %q", call.mediaId, id)
	}
	if call.organisationId != target().OrganisationId {
		t.Errorf("organisationId = %q, want %q stamped from target", call.organisationId, target().OrganisationId)
	}
	// The wire field names are mapped to their media-document paths.
	if got, ok := call.fields["metadata.description"]; !ok || got != "a new description" {
		t.Errorf("fields[metadata.description] = %v (ok=%v), want the description", got, ok)
	}
	if got, ok := call.fields["star"]; !ok || got != true {
		t.Errorf("fields[star] = %v (ok=%v), want true", got, ok)
	}
	if _, ok := call.fields["mediaId"]; ok {
		t.Error("mediaId must not leak into the patched fields")
	}
	if len(batch.Blocks) != 1 || batch.Blocks[0].Type != KindMediaPatch {
		t.Fatalf("want 1 media-patch block report, got %+v", batch.Blocks)
	}
	if d, ok := batch.Blocks[0].Detail.(MediaPatchDetail); !ok || d.Fields != 2 {
		t.Errorf("detail = %#v, want MediaPatchDetail{Fields:2}", batch.Blocks[0].Detail)
	}
}

func TestIngestBlocks_MediaPatchByKey(t *testing.T) {
	store := &fakeMediaPatcher{}
	scope := Scope{Source: SourcePipeline, Media: store}

	_, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
		Blocks: []Block{mediaPatchBlock(t, map[string]any{
			"mediaKey":    "1712000000_6-967003_device_0-0-0-0_60_1000.mp4",
			"description": "Number plate detected: ABC123",
		})},
	})
	if err != nil {
		t.Fatalf("IngestBlocks: %v", err)
	}
	if len(store.calls) != 1 {
		t.Fatalf("want 1 patch call, got %d", len(store.calls))
	}
	call := store.calls[0]
	if call.mediaId != "" {
		t.Errorf("mediaId = %q, want empty when only a key is supplied", call.mediaId)
	}
	if call.mediaKey != "1712000000_6-967003_device_0-0-0-0_60_1000.mp4" {
		t.Errorf("mediaKey = %q, want the recording key passed through", call.mediaKey)
	}
	if got, ok := call.fields["metadata.description"]; !ok || got != "Number plate detected: ABC123" {
		t.Errorf("fields[metadata.description] = %v (ok=%v), want the description", got, ok)
	}
	if _, ok := call.fields["mediaKey"]; ok {
		t.Error("mediaKey must not leak into the patched fields")
	}
}

func TestIngestBlocks_MediaPatchPrefersIdOverKey(t *testing.T) {
	store := &fakeMediaPatcher{}
	scope := Scope{Source: SourcePipeline, Media: store}

	id := primitive.NewObjectID().Hex()
	_, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
		Blocks: []Block{mediaPatchBlock(t, map[string]any{
			"mediaId":     id,
			"mediaKey":    "some-key.mp4",
			"description": "x",
		})},
	})
	if err != nil {
		t.Fatalf("IngestBlocks: %v", err)
	}
	if len(store.calls) != 1 {
		t.Fatalf("want 1 patch call, got %d", len(store.calls))
	}
	// Both identifiers are passed through; the sink prefers the id. The decode
	// still validates the id even when a key is also present.
	if store.calls[0].mediaId != id {
		t.Errorf("mediaId = %q, want %q", store.calls[0].mediaId, id)
	}
	if store.calls[0].mediaKey != "some-key.mp4" {
		t.Errorf("mediaKey = %q, want it passed through alongside the id", store.calls[0].mediaKey)
	}
}

func TestIngestBlocks_MediaPatchRejectsAPISource(t *testing.T) {
	store := &fakeMediaPatcher{}
	scope := Scope{Source: SourceAPI, Media: store}

	_, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
		Blocks: []Block{mediaPatchBlock(t, map[string]any{
			"mediaId":     primitive.NewObjectID().Hex(),
			"description": "x",
		})},
	})
	if !errors.Is(err, ErrSourceNotAllowed) {
		t.Fatalf("err = %v, want ErrSourceNotAllowed (media-patch is pipeline-only)", err)
	}
	if len(store.calls) != 0 {
		t.Errorf("want nothing patched when the source is forbidden, got %d", len(store.calls))
	}
}

func TestDecodeMediaPatch_Validation(t *testing.T) {
	validId := primitive.NewObjectID().Hex()
	cases := map[string]map[string]any{
		"missing id and key":       {"description": "x"},
		"blank id and key":         {"mediaId": "  ", "mediaKey": "  ", "description": "x"},
		"invalid mediaId":          {"mediaId": "123", "description": "x"},
		"invalid id even with key": {"mediaId": "123", "mediaKey": "k.mp4", "description": "x"},
		"no fields":                {"mediaId": validId},
		"key but no fields":        {"mediaKey": "k.mp4"},
		"unknown field":            {"mediaId": validId, "organisationId": "other-org"},
		"not-patchable _id":        {"mediaId": validId, "id": "x"},
	}
	scope := Scope{Source: SourcePipeline, Media: &fakeMediaPatcher{}}
	for label, body := range cases {
		t.Run(label, func(t *testing.T) {
			_, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
				Blocks: []Block{mediaPatchBlock(t, body)},
			})
			if !errors.Is(err, ErrMediaPatchValidation) {
				t.Fatalf("err = %v, want ErrMediaPatchValidation", err)
			}
		})
	}
}

func TestDecodeMediaPatch_RejectsEmptyOrganisation(t *testing.T) {
	store := &fakeMediaPatcher{}
	scope := Scope{Source: SourcePipeline, Media: store}
	tgt := target()
	tgt.OrganisationId = "  "

	_, err := IngestBlocks(context.Background(), scope, tgt, BlockEnvelope{
		Blocks: []Block{mediaPatchBlock(t, map[string]any{
			"mediaId":     primitive.NewObjectID().Hex(),
			"description": "x",
		})},
	})
	if !errors.Is(err, ErrMediaPatchValidation) {
		t.Fatalf("err = %v, want ErrMediaPatchValidation", err)
	}
	if len(store.calls) != 0 {
		t.Errorf("empty organisation must not reach the media sink, got %d call(s)", len(store.calls))
	}
}

func TestDecodeMediaPatch_ValidatesFieldTypes(t *testing.T) {
	validId := primitive.NewObjectID().Hex()
	cases := map[string]map[string]any{
		"null description":      {"mediaId": validId, "description": nil},
		"object description":    {"mediaId": validId, "description": map[string]any{"text": "x"}},
		"null star":             {"mediaId": validId, "star": nil},
		"string star":           {"mediaId": validId, "star": "true"},
		"null tag names":        {"mediaId": validId, "tagNames": nil},
		"scalar tag names":      {"mediaId": validId, "tagNames": "tag"},
		"non-string tag name":   {"mediaId": validId, "tagNames": []any{"tag", 1}},
		"non-string event name": {"mediaId": validId, "eventNames": []any{false}},
		"object marker names":   {"mediaId": validId, "markerNames": map[string]any{}},
	}
	scope := Scope{Source: SourcePipeline, Media: &fakeMediaPatcher{}}
	for label, body := range cases {
		t.Run(label, func(t *testing.T) {
			_, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
				Blocks: []Block{mediaPatchBlock(t, body)},
			})
			if !errors.Is(err, ErrMediaPatchValidation) {
				t.Fatalf("err = %v, want ErrMediaPatchValidation", err)
			}
		})
	}
}

func TestDecodeMediaPatch_AcceptsDocumentedFieldTypes(t *testing.T) {
	store := &fakeMediaPatcher{}
	scope := Scope{Source: SourcePipeline, Media: store}

	_, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
		Blocks: []Block{mediaPatchBlock(t, map[string]any{
			"mediaId":     primitive.NewObjectID().Hex(),
			"description": "",
			"star":        false,
			"tagNames":    []string{},
			"eventNames":  []string{"motion"},
			"markerNames": []string{"person"},
		})},
	})
	if err != nil {
		t.Fatalf("IngestBlocks: %v", err)
	}
	if len(store.calls) != 1 {
		t.Fatalf("want 1 patch call, got %d", len(store.calls))
	}
	if _, ok := store.calls[0].fields["tagNames"].([]string); !ok {
		t.Errorf("tagNames type = %T, want []string", store.calls[0].fields["tagNames"])
	}
}

func TestApplyMediaPatch_NoStore(t *testing.T) {
	scope := Scope{Source: SourcePipeline} // Media nil
	_, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
		Blocks: []Block{mediaPatchBlock(t, map[string]any{
			"mediaId":     primitive.NewObjectID().Hex(),
			"description": "x",
		})},
	})
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent when no MediaPatcher is configured", err)
	}
}
