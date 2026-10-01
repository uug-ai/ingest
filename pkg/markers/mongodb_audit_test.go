package markers

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestMarkerWithWriteAudit(t *testing.T) {
	now := time.Date(2026, 10, 1, 8, 0, 0, 123000000, time.UTC)
	old := now.Add(-time.Hour)
	for _, supplied := range []*models.Audit{
		nil,
		{},
		{CreatedAt: old, UpdatedAt: old, CreatedBy: "creator", UpdatedBy: "worker", LastAction: "marker.created"},
	} {
		var before models.Audit
		if supplied != nil {
			before = *supplied
		}
		marker := markerWithWriteAudit(models.Marker{Audit: supplied}, now)
		if marker.Audit == nil || marker.Audit == supplied {
			t.Fatal("writer must allocate its own audit")
		}
		if marker.Audit.CreatedAt != now || marker.Audit.UpdatedAt != now {
			t.Fatalf("write-time audit = %#v", marker.Audit)
		}
		if marker.Audit.CreatedBy != before.CreatedBy || marker.Audit.UpdatedBy != before.UpdatedBy || marker.Audit.LastAction != before.LastAction {
			t.Fatalf("caller provenance changed: %#v", marker.Audit)
		}
		if supplied != nil && *supplied != before {
			t.Fatalf("caller audit mutated: %#v", supplied)
		}
	}
}

func TestMarkerWriterAudit(t *testing.T) {
	uri := os.Getenv("MARKERS_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("set MARKERS_TEST_MONGO_URI to run the markers audit integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Disconnect(context.Background()) }()
	dbName := fmt.Sprintf("markers_audit_test_%d", time.Now().UnixNano())
	origDB := DatabaseName
	DatabaseName = dbName
	defer func() {
		DatabaseName = origDB
		_ = client.Database(dbName).Drop(context.Background())
	}()
	collection := client.Database(dbName).Collection(MARKERS_COLLECTION)
	organisation := primitive.NewObjectID()
	project := primitive.NewObjectID()
	base := models.Marker{
		OrganisationId: organisation.Hex(), ProjectId: &project,
		DeviceId: "audit-device", StartTimestamp: 1000, EndTimestamp: 1010,
	}
	old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	assertStored := func(t *testing.T, marker models.Marker) {
		t.Helper()
		var stored models.Marker
		if err := collection.FindOne(ctx, bson.M{"_id": marker.Id}).Decode(&stored); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(stored.Audit, marker.Audit) {
			t.Fatalf("returned audit %#v differs from stored %#v", marker.Audit, stored.Audit)
		}
		if stored.OrganisationId != base.OrganisationId || stored.ProjectId == nil || *stored.ProjectId != project {
			t.Fatalf("canonical ownership changed: %#v", stored)
		}
	}
	assertWriteTime := func(t *testing.T, stamp, before time.Time) {
		t.Helper()
		if stamp.Before(before) || stamp.After(time.Now().UTC()) || stamp.Location() != time.UTC {
			t.Fatalf("stamp %s is not UTC write time after %s", stamp, before)
		}
	}

	for _, upsert := range []bool{false, true} {
		for _, withAudit := range []bool{false, true} {
			t.Run(fmt.Sprintf("new/upsert=%t/audit=%t", upsert, withAudit), func(t *testing.T) {
				marker := base
				marker.Name = t.Name()
				if withAudit {
					marker.Audit = &models.Audit{CreatedAt: old, UpdatedAt: old, CreatedBy: "$creator", UpdatedBy: "$worker", LastAction: "$created"}
				}
				var original models.Audit
				if marker.Audit != nil {
					original = *marker.Audit
				}
				before := time.Now().UTC().Truncate(time.Millisecond)
				got, err := addMarker(ctx, nil, client, marker, upsert)
				if err != nil {
					t.Fatal(err)
				}
				if got.Audit == nil {
					t.Fatal("missing audit")
				}
				assertWriteTime(t, got.Audit.UpdatedAt, before)
				if got.Audit.CreatedAt != got.Audit.UpdatedAt {
					t.Fatalf("new marker timestamps differ: %#v", got.Audit)
				}
				if withAudit {
					if *marker.Audit != original {
						t.Fatal("caller audit mutated")
					}
					if got.Audit.CreatedBy != original.CreatedBy || got.Audit.UpdatedBy != original.UpdatedBy || got.Audit.LastAction != original.LastAction {
						t.Fatalf("creation provenance changed: %#v", got.Audit)
					}
				}
				assertStored(t, got)
			})
		}
	}

	t.Run("replay preserves creation audit", func(t *testing.T) {
		marker := base
		marker.Name = "$literal-marker"
		marker.Description = "$literal-description"
		marker.Audit = &models.Audit{CreatedBy: "$original"}
		first, err := UpsertMarkerToMongodb(ctx, nil, client, marker)
		if err != nil {
			t.Fatal(err)
		}
		for _, incoming := range []*models.Audit{
			{CreatedAt: old, UpdatedAt: old, CreatedBy: "$replacement", UpdatedBy: "$replay", LastAction: "$updated"},
			nil,
		} {
			time.Sleep(2 * time.Millisecond)
			marker.Audit = incoming
			var original models.Audit
			if incoming != nil {
				original = *incoming
			}
			got, err := UpsertMarkerToMongodb(ctx, nil, client, marker)
			if err != nil {
				t.Fatal(err)
			}
			if got.Id != first.Id || got.Audit.CreatedAt != first.Audit.CreatedAt || got.Audit.CreatedBy != "$original" {
				t.Fatalf("replay replaced identity or creation audit: %#v", got)
			}
			if !got.Audit.UpdatedAt.After(first.Audit.UpdatedAt) {
				t.Fatalf("updatedAt did not advance: %#v", got.Audit)
			}
			if got.Name != marker.Name || got.Description != marker.Description || got.Audit.UpdatedBy != "$replay" || got.Audit.LastAction != "$updated" {
				t.Fatalf("literal fields or update provenance changed: %#v", got)
			}
			if incoming != nil && *incoming != original {
				t.Fatal("replay mutated caller audit")
			}
			assertStored(t, got)
			first = got
		}
	})

	for _, legacy := range []struct {
		name  string
		audit any
	}{
		{"missing", nil},
		{"null", nil},
		{"empty", bson.M{}},
		{"empty fields", bson.M{"createdAt": "", "createdBy": ""}},
		{"zero fields", bson.M{"createdAt": time.Time{}, "createdBy": ""}},
		{"existing creation", bson.M{"createdAt": old, "createdBy": "legacy-creator"}},
	} {
		t.Run("legacy/"+legacy.name, func(t *testing.T) {
			marker := base
			marker.Name = t.Name()
			seed, err := markerSetDoc(marker)
			if err != nil {
				t.Fatal(err)
			}
			if legacy.name != "missing" {
				seed["audit"] = legacy.audit
			}
			if _, err := collection.InsertOne(ctx, seed); err != nil {
				t.Fatal(err)
			}
			marker.Audit = &models.Audit{CreatedAt: old.Add(-time.Hour), UpdatedAt: old, CreatedBy: "incoming"}
			before := time.Now().UTC().Truncate(time.Millisecond)
			got, err := UpsertMarkerToMongodb(ctx, nil, client, marker)
			if err != nil {
				t.Fatal(err)
			}
			assertWriteTime(t, got.Audit.UpdatedAt, before)
			if legacy.name == "existing creation" {
				if got.Audit.CreatedAt != old || got.Audit.CreatedBy != "legacy-creator" {
					t.Fatalf("legacy creation audit overwritten: %#v", got.Audit)
				}
			} else if got.Audit.CreatedAt != got.Audit.UpdatedAt || got.Audit.CreatedBy != "incoming" {
				t.Fatalf("legacy creation audit not initialized: %#v", got.Audit)
			}
			assertStored(t, got)
		})
	}

	t.Run("post-image decode errors are returned", func(t *testing.T) {
		marker := base
		marker.Name = t.Name()
		seed, err := markerSetDoc(marker)
		if err != nil {
			t.Fatal(err)
		}
		seed["audit"] = bson.M{"createdAt": "invalid-date"}
		if _, err := collection.InsertOne(ctx, seed); err != nil {
			t.Fatal(err)
		}
		if _, err := UpsertMarkerToMongodb(ctx, nil, client, marker); err == nil {
			t.Fatal("expected invalid stored creation date to surface as a decode error")
		}
	})
}
