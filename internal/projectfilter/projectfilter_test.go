package projectfilter

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TestPredicate_UnsetProjectDoesNotScope pins that an absent project leaves a
// query organisation-wide rather than scoping it to "no project": an unset
// project means the caller could not resolve one, and a filter that demanded a
// null projectId would exclude every correctly stamped document.
func TestPredicate_UnsetProjectDoesNotScope(t *testing.T) {
	if got := Predicate(nil); got != nil {
		t.Errorf("Predicate(nil) = %v, want nil", got)
	}
	zero := primitive.NilObjectID
	if got := Predicate(&zero); got != nil {
		t.Errorf("Predicate(zero) = %v, want nil (a zero id is not a project)", got)
	}
}

// TestPredicate_KeepsUnstampedDocumentsReachable is the regression test for the
// tolerant form. MongoDB matches a missing field against null, so the null arm is
// what lets a document written before projectId existed still match. Without it
// an upsert keyed on this predicate would fail to find its own earlier document
// and insert a duplicate, and the marker→media link would tag nothing.
func TestPredicate_KeepsUnstampedDocumentsReachable(t *testing.T) {
	projectId := primitive.NewObjectID()

	predicate := Predicate(&projectId)
	if predicate == nil {
		t.Fatal("Predicate(set) = nil, want a scoping predicate")
	}
	values, ok := predicate["$in"].(bson.A)
	if !ok {
		t.Fatalf("predicate = %#v, want an $in", predicate)
	}
	if len(values) != 2 {
		t.Fatalf("$in = %#v, want exactly the project and the null arm", values)
	}
	if values[0] != projectId {
		t.Errorf("$in[0] = %v, want %v", values[0], projectId)
	}
	if values[1] != nil {
		t.Errorf("$in[1] = %v, want nil — the arm that keeps pre-rollout documents reachable", values[1])
	}
}

// TestApply_UsesItsOwnTopLevelKey pins the composition rule: the predicate never
// occupies $or, because the marker filters already scope the device that way and
// a second $or would silently overwrite the first.
func TestApply_UsesItsOwnTopLevelKey(t *testing.T) {
	projectId := primitive.NewObjectID()
	deviceScope := []bson.M{{"deviceKey": "device-1"}}

	filter := Apply(bson.M{"$or": deviceScope, "organisationId": "org-1"}, &projectId)

	if _, scoped := filter["projectId"]; !scoped {
		t.Error("Apply must set the projectId key")
	}
	if got, ok := filter["$or"].([]bson.M); !ok || len(got) != 1 {
		t.Errorf("$or = %#v, want the device scope untouched", filter["$or"])
	}
	if filter["organisationId"] != "org-1" {
		t.Errorf("organisationId = %v, want org-1", filter["organisationId"])
	}
}

// TestApply_LeavesFilterUntouchedWithoutAProject checks the no-op path: a caller
// that has no project keeps exactly the filter it built.
func TestApply_LeavesFilterUntouchedWithoutAProject(t *testing.T) {
	filter := Apply(bson.M{"organisationId": "org-1"}, nil)

	if len(filter) != 1 || filter["organisationId"] != "org-1" {
		t.Errorf("filter = %#v, want only the organisation clause", filter)
	}
}
