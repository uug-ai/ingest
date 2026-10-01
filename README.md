# Ingest

[![Go Version](https://img.shields.io/badge/Go-1.25-blue.svg)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![GoDoc](https://godoc.org/github.com/uug-ai/ingest?status.svg)](https://godoc.org/github.com/uug-ai/ingest)
[![Go Report Card](https://goreportcard.com/badge/github.com/uug-ai/ingest)](https://goreportcard.com/report/github.com/uug-ai/ingest)
[![Release](https://img.shields.io/github/release/uug-ai/ingest.svg)](https://github.com/uug-ai/ingest/releases/latest)

The shared **ingest core** for the platform: a deliberately infra-free library that
routes self-describing *block envelopes* to typed handlers and runs each block's
ordered, idempotent actions against the sink interfaces it declares. The same core
runs whether a result arrives over the workflows queue (a pipeline stage) or the
public API (`POST /ingest`) — only the trust level differs.

The package depends only on the model types (`github.com/uug-ai/models`) and on the
**sink interfaces** it declares (`DetectionStore`, `MarkerStore`, `RegionPromoter`).
Each application supplies the concrete persistence implementation when it builds the
`Scope` it passes in, so routing has one implementation while persistence stays in
each app's repository layer.

## Features

• **Block routing** — one dispatcher fans every block out to its handler by `type`; the dispatcher never grows a `case`.
• **Typed handlers** — built-in `detection`, `marker` and `media-patch` block kinds, each with ordered, idempotent actions.
• **Infra-free** — no database or transport dependencies; callers inject sinks via the `Scope`.
• **Fail-fast validation** — a pre-pass rejects unknown kinds, disallowed sources, oversized payloads, and malformed block bodies before any side effects.
• **Source-aware** — the same core enforces different rules for `api` vs `pipeline` callers.

## Installation

```bash
go get github.com/uug-ai/ingest
```

## Quick Start

```go
package main

import (
	"context"
	"encoding/json"

	"github.com/uug-ai/ingest/pkg/ingest"
)

func run(ctx context.Context, detections ingest.DetectionStore, regions ingest.RegionPromoter, payload json.RawMessage) error {
	scope := ingest.Scope{
		Source:     ingest.SourcePipeline,
		Detections: detections,
		Regions:    regions,
	}

	target := ingest.Target{
		Key:            "recording-key",
		OrganisationId: "org-id",
		DeviceId:       "device-id",
	}

	env := ingest.BlockEnvelope{
		Blocks: []ingest.Block{{Type: ingest.KindDetection, Data: payload}},
	}

	_, err := ingest.IngestBlocks(ctx, scope, target, env)
	return err
}
```

## Package Layout

```
pkg/ingest/
├── ingest.go       # dispatcher, envelope/block types, handler registry, IngestBlocks
├── detection.go    # detection block handler + actions (DetectionStore, RegionPromoter)
├── marker.go       # marker block handler + action (MarkerStore)
└── media_patch.go  # media-patch block handler + action (MediaPatcher)
```

The concrete Mongo sinks live one layer up, in sibling packages the composition
root wires through `pkg/ingeststore`: `pkg/detections`, `pkg/markers`, and
`pkg/media` (the media-patch `$set` writer).

### Marker write timestamps

Both shared marker writers stamp `audit.updatedAt` with the writer's current UTC
time (BSON date, millisecond precision), including unchanged replays. Fresh
inserts initialize `audit.createdAt` to that same write time and retain supplied
actor/action provenance. Upserts atomically preserve stored nonempty `createdAt`
and `createdBy`; missing, null, empty, or zero creation stamps are initialized
when touched. Caller-owned audit objects are never mutated. The upsert returns
the stored post-image, including its audit, and propagates read/decode errors.

Upserts use MongoDB update pipelines (MongoDB 4.2+). Deploy this writer to **all**
marker-producing services before relying on incremental polling of
`audit.updatedAt`. Existing untouched markers are not backfilled: bootstrap a
snapshot before polling. Timestamps reflect writer clocks, not event time or a
strictly increasing sequence; keep clocks synchronized and use an overlap window
with deduplication for equal-millisecond writes, clock skew, and in-flight writes.
No ownership migration is required.

## Development

This module is part of the platform monorepo and resolves its sibling modules
through a local `go.work` file during development. Outside the workspace it builds
against the published `github.com/uug-ai/models` release pinned in `go.mod`.

```bash
go build ./...
go test ./...
```

Marker writer integration tests run when `MARKERS_TEST_MONGO_URI` is set; they
use isolated databases and remove them afterward.
