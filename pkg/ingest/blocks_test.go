package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

// Ingest is a test-only single-block convenience over IngestBlocks. Production
// code has no per-kind entry point — a producer emits a BlockEnvelope — but the
// per-kind decode/normalise tests read most clearly as one typed call, so this
// wrapper (compiled only into the test binary) preserves them.
func Ingest(ctx context.Context, scope Scope, tgt Target, kind string, payload json.RawMessage) (Report, error) {
	batch, err := IngestBlocks(ctx, scope, tgt, BlockEnvelope{
		Blocks: []Block{{Type: kind, Data: payload}},
	})
	if err != nil {
		return Report{}, err
	}
	if len(batch.Blocks) == 0 {
		return Report{}, nil
	}
	return batch.Blocks[0].Report, nil
}

// detectionBlock builds a one-track normalized detection block with the given
// run id, so a test can compose envelopes of several distinct detection runs.
func detectionBlock(t *testing.T, runId string) Block {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"source":          map[string]any{"runId": runId},
		"coordinateSpace": "normalized",
		"tracks": []any{map[string]any{"id": "t", "boxes": []any{
			map[string]any{"frame": 0, "x1": 0.1, "y1": 0.1, "x2": 0.2, "y2": 0.2},
		}}},
	})
	if err != nil {
		t.Fatalf("marshal detection block: %v", err)
	}
	return Block{Type: KindDetection, Data: body}
}

func TestIngestBlocks_MultipleHeterogeneousBlocks(t *testing.T) {
	store := &fakeStore{}
	scope := Scope{Source: SourcePipeline, Detections: store, Regions: &fakePromoter{}}

	batch, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
		Blocks: []Block{detectionBlock(t, "RUN-1"), detectionBlock(t, "RUN-2")},
	})
	if err != nil {
		t.Fatalf("IngestBlocks: %v", err)
	}
	if len(batch.Blocks) != 2 {
		t.Fatalf("want 2 block reports, got %d", len(batch.Blocks))
	}
	if len(store.runs) != 2 {
		t.Fatalf("want 2 stored detection runs, got %d", len(store.runs))
	}
	if batch.Blocks[0].RunId != "RUN-1" || batch.Blocks[1].RunId != "RUN-2" {
		t.Errorf("block run ids = %q,%q want RUN-1,RUN-2", batch.Blocks[0].RunId, batch.Blocks[1].RunId)
	}
}

func TestIngestBlocks_UnknownTypeRejectsWholeEnvelope(t *testing.T) {
	store := &fakeStore{}
	scope := Scope{Source: SourcePipeline, Detections: store, Regions: &fakePromoter{}}

	_, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
		Blocks: []Block{detectionBlock(t, "RUN-1"), {Type: "bogus", Data: json.RawMessage(`{}`)}},
	})
	if !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("err = %v, want ErrUnknownKind", err)
	}
	if len(store.runs) != 0 {
		t.Errorf("want nothing stored when an envelope is rejected, got %d", len(store.runs))
	}
}

func TestIngestBlocks_PersistErrorTagged(t *testing.T) {
	store := &fakeStore{err: errors.New("mongo down")}
	scope := Scope{Source: SourcePipeline, Detections: store, Regions: &fakePromoter{}}

	_, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
		Blocks: []Block{detectionBlock(t, "RUN-1")},
	})
	if !errors.Is(err, ErrPersist) {
		t.Fatalf("err = %v, want ErrPersist (retryable sink failure)", err)
	}
}

func TestIngestBlocks_TooManyBlocks(t *testing.T) {
	scope := Scope{Source: SourcePipeline, Detections: &fakeStore{}, Regions: &fakePromoter{}}
	blocks := make([]Block, maxBlocksPerPayload+1)
	for i := range blocks {
		blocks[i] = Block{Type: KindDetection, Data: json.RawMessage(`{}`)}
	}
	_, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{Blocks: blocks})
	if !errors.Is(err, ErrTooManyBlocks) {
		t.Fatalf("err = %v, want ErrTooManyBlocks", err)
	}
}

func TestHandlerAllowsSource(t *testing.T) {
	// Empty allow-list defaults to trusted pipeline only.
	pipelineOnly := Handler{Kind: "x"}
	if pipelineOnly.AllowsSource(SourceAPI) {
		t.Error("empty AllowedSources must not permit SourceAPI")
	}
	if !pipelineOnly.AllowsSource(SourcePipeline) {
		t.Error("empty AllowedSources must permit SourcePipeline")
	}
	// An explicit list is honoured.
	both := Handler{Kind: "y", AllowedSources: []Source{SourceAPI, SourcePipeline}}
	if !both.AllowsSource(SourceAPI) || !both.AllowsSource(SourcePipeline) {
		t.Error("explicit AllowedSources must permit listed sources")
	}
}

// permanentSinkErr is a sink error that declares itself non-retryable via the
// Permanent() bool behaviour the ingest core recognises — the markers sink uses
// the same shape, without importing this package.
type permanentSinkErr struct{ msg string }

func (e permanentSinkErr) Error() string  { return e.msg }
func (e permanentSinkErr) Permanent() bool { return true }

func TestIsPermanent(t *testing.T) {
	if IsPermanent(nil) {
		t.Error("IsPermanent(nil) = true, want false")
	}
	if !IsPermanent(ErrPermanent) {
		t.Error("IsPermanent(ErrPermanent) = false, want true")
	}
	if !IsPermanent(fmt.Errorf("wrap: %w", ErrPermanent)) {
		t.Error("IsPermanent(wrapped ErrPermanent) = false, want true")
	}
	if !IsPermanent(permanentSinkErr{msg: "validator"}) {
		t.Error("IsPermanent(Permanent()=true) = false, want true")
	}
	if IsPermanent(errors.New("mongo down")) {
		t.Error("IsPermanent(plain error) = true, want false")
	}
	if IsPermanent(ErrPersist) {
		t.Error("IsPermanent(ErrPersist) = true, want false")
	}
}

// TestIngestBlocks_PermanentErrorNotRetried locks in the fix: a deterministic
// sink rejection is tagged ErrPermanent (drop) and never ErrPersist (retry), so
// the queue can no longer be looped forever on a write that can never succeed.
func TestIngestBlocks_PermanentErrorNotRetried(t *testing.T) {
	store := &fakeStore{err: permanentSinkErr{msg: "document failed validation"}}
	scope := Scope{Source: SourcePipeline, Detections: store, Regions: &fakePromoter{}}

	_, err := IngestBlocks(context.Background(), scope, target(), BlockEnvelope{
		Blocks: []Block{detectionBlock(t, "RUN-1")},
	})
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent", err)
	}
	if errors.Is(err, ErrPersist) {
		t.Errorf("permanent failure must not also be tagged ErrPersist: %v", err)
	}
	if !IsPermanent(err) {
		t.Errorf("IsPermanent(returned err) = false, want true")
	}
}
