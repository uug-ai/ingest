package ingest

import (
	"encoding/json"
	"testing"

	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TestResolveTargetProject covers the three placements a target can produce.
func TestResolveTargetProject(t *testing.T) {
	organisationId := primitive.NewObjectID()

	t.Run("explicit project wins", func(t *testing.T) {
		projectId := primitive.NewObjectID()
		got := resolveTargetProject(Target{OrganisationId: organisationId.Hex(), ProjectId: &projectId})
		if got == nil || *got != projectId {
			t.Fatalf("project = %v, want the target's explicit %v", got, projectId)
		}
	})

	t.Run("falls back to the organisation default", func(t *testing.T) {
		got := resolveTargetProject(Target{OrganisationId: organisationId.Hex()})
		want := models.DefaultProjectId(organisationId)
		if got == nil || *got != want {
			t.Fatalf("project = %v, want the computed default %v", got, want)
		}
	})

	t.Run("a zero explicit project falls back too", func(t *testing.T) {
		zero := primitive.NilObjectID
		got := resolveTargetProject(Target{OrganisationId: organisationId.Hex(), ProjectId: &zero})
		want := models.DefaultProjectId(organisationId)
		if got == nil || *got != want {
			t.Fatalf("project = %v, want the computed default %v for a zero id", got, want)
		}
	})

	// A guess here would be worse than nothing: a fabricated project hides the
	// resource from every project-scoped read, whereas leaving it unstamped keeps
	// it organisation-wide and still resolvable by the tolerant read predicates.
	t.Run("a non-ObjectID organisation yields no project", func(t *testing.T) {
		if got := resolveTargetProject(Target{OrganisationId: "org-9"}); got != nil {
			t.Fatalf("project = %v, want nil rather than a fabricated project", got)
		}
	})
}

// TestDecodeMarker_ProjectIsServerOwned is the trust-hole regression.
// models.Marker.ProjectId is json-decodable (`projectId,omitempty`), so a
// producer can put one on the wire — and if decodeMarker did not overwrite it,
// that producer would choose which project its marker lands in. The tenant
// placement is the server's to decide, exactly as it already is for the
// organisation and the document id.
func TestDecodeMarker_ProjectIsServerOwned(t *testing.T) {
	organisationId := primitive.NewObjectID()
	targetProject := primitive.NewObjectID()
	wireProject := primitive.NewObjectID()

	payload, err := json.Marshal(map[string]any{
		"name":           "ABC-123",
		"startTimestamp": 100,
		"endTimestamp":   105,
		"projectId":      wireProject.Hex(),
	})
	if err != nil {
		t.Fatalf("marshal marker: %v", err)
	}

	run, _, err := decodeMarker(Scope{}, Target{
		OrganisationId: organisationId.Hex(),
		ProjectId:      &targetProject,
	}, payload)
	if err != nil {
		t.Fatalf("decodeMarker: %v", err)
	}
	marker, ok := run.(models.Marker)
	if !ok {
		t.Fatalf("run = %T, want models.Marker", run)
	}
	if marker.ProjectId == nil {
		t.Fatal("projectId = nil, want the target's project")
	}
	if *marker.ProjectId == wireProject {
		t.Fatal("the wire-supplied projectId was trusted: a producer must not be able to place its marker in a project of its own choosing")
	}
	if *marker.ProjectId != targetProject {
		t.Errorf("projectId = %v, want %v from the target", *marker.ProjectId, targetProject)
	}
}

// TestDecodeMarker_StampsDefaultProject covers the common path today: an adapter
// that has not resolved a project still produces a marker every project-scoped
// reader can find, because the default is derived from the organisation.
func TestDecodeMarker_StampsDefaultProject(t *testing.T) {
	organisationId := primitive.NewObjectID()

	payload, err := json.Marshal(map[string]any{"name": "ABC-123", "startTimestamp": 100})
	if err != nil {
		t.Fatalf("marshal marker: %v", err)
	}

	run, _, err := decodeMarker(Scope{}, Target{OrganisationId: organisationId.Hex()}, payload)
	if err != nil {
		t.Fatalf("decodeMarker: %v", err)
	}
	marker := run.(models.Marker)
	want := models.DefaultProjectId(organisationId)
	if marker.ProjectId == nil || *marker.ProjectId != want {
		t.Errorf("projectId = %v, want the organisation default %v", marker.ProjectId, want)
	}
}

// TestDecodeDetection_StampsProject pins that the run carries the project too.
// models.DetectionRun.ProjectId is `json:"-"`, so there is no wire to distrust
// here — only a gap to fill. The stamp must survive Normalize, which builds a
// fresh run and would discard anything set before it.
func TestDecodeDetection_StampsProject(t *testing.T) {
	organisationId := primitive.NewObjectID()
	projectId := primitive.NewObjectID()

	run, _, err := decodeDetection(Scope{Source: SourcePipeline}, Target{
		Key:            "cam-1_1700000000_rec",
		OrganisationId: organisationId.Hex(),
		ProjectId:      &projectId,
	}, pixelPayload(t))
	if err != nil {
		t.Fatalf("decodeDetection: %v", err)
	}
	detection, ok := run.(models.DetectionRun)
	if !ok {
		t.Fatalf("run = %T, want models.DetectionRun", run)
	}
	if detection.ProjectId == nil || *detection.ProjectId != projectId {
		t.Errorf("projectId = %v, want %v from the target", detection.ProjectId, projectId)
	}
	if detection.OrganisationId != organisationId.Hex() {
		t.Errorf("organisationId = %q, want %q", detection.OrganisationId, organisationId.Hex())
	}
}

// TestPatchMedia_PassesTargetProjectToSink pins that the media-patch action scopes
// the update by the target's project rather than leaving it organisation-wide.
func TestPatchMedia_PassesTargetProjectToSink(t *testing.T) {
	organisationId := primitive.NewObjectID()
	projectId := primitive.NewObjectID()
	patcher := &fakeMediaPatcher{}

	err := PatchMedia{}.Apply(t.Context(), Scope{Source: SourcePipeline, Media: patcher}, Target{
		OrganisationId: organisationId.Hex(),
		ProjectId:      &projectId,
	}, MediaPatch{MediaKey: "org/recording.mp4", Set: map[string]any{"star": true}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(patcher.calls) != 1 {
		t.Fatalf("want 1 patch, got %d", len(patcher.calls))
	}
	got := patcher.calls[0].projectId
	if got == nil || *got != projectId {
		t.Errorf("projectId = %v, want %v from the target", got, projectId)
	}
}
