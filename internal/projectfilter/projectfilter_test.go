package projectfilter

import (
	"testing"

	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TestClause_UnsetProjectDoesNotScope pins that an absent project leaves a query
// organisation-wide rather than scoping it to "no project": an unset project
// means the caller could not resolve one, and a filter that demanded a null
// projectId would exclude every correctly stamped document.
func TestClause_UnsetProjectDoesNotScope(t *testing.T) {
	organisationId := primitive.NewObjectID().Hex()

	if got := Clause(organisationId, nil); got != nil {
		t.Errorf("Clause(nil) = %v, want nil", got)
	}
	zero := primitive.NilObjectID
	if got := Clause(organisationId, &zero); got != nil {
		t.Errorf("Clause(zero) = %v, want nil (a zero id is not a project)", got)
	}

	// An organisation that is not an ObjectID cannot be told apart from its own
	// default project, so the clause degrades to organisation-wide rather than to
	// a predicate that matches nothing.
	projectId := primitive.NewObjectID()
	if got := Clause("not-an-objectid", &projectId); got != nil {
		t.Errorf("Clause(non-ObjectID org) = %v, want nil", got)
	}
}

// TestClause_ToleratesUnstampedOnlyForTheDefaultProject is the regression this
// package was rewritten for.
//
// The default project must keep matching documents written before projectId
// existed: they belong to it, and a strict clause would make an organisation's
// entire pre-rollout history vanish — and would make the writers' own upserts
// fail to find their earlier document and insert duplicates instead.
//
// A NON-default project must not. Ingest used to relax unconditionally, which
// reads identically today (during the rollout every project is its
// organisation's default) but leaks the moment a second project exists: that
// project's queries would match every unstamped document in the organisation.
func TestClause_ToleratesUnstampedOnlyForTheDefaultProject(t *testing.T) {
	organisation := primitive.NewObjectID()
	organisationId := organisation.Hex()

	t.Run("the default project also matches unstamped documents", func(t *testing.T) {
		defaultProject := models.DefaultProjectId(organisation)

		clause := Clause(organisationId, &defaultProject)
		assertTolerantDefaultProjectClause(t, clause, defaultProject)
	})

	t.Run("a real project matches only what is stamped for it", func(t *testing.T) {
		realProject := primitive.NewObjectID()

		clause := Clause(organisationId, &realProject)
		if got := clause["projectId"]; got != realProject {
			t.Errorf("clause = %#v, want strict equality on %v", clause, realProject)
		}
		if _, tolerant := clause["$or"]; tolerant {
			t.Errorf("clause = %#v, want no $or: a null arm here would match every "+
				"unstamped document in the organisation, which is a cross-project leak", clause)
		}
		if len(clause) != 1 {
			t.Errorf("clause = %#v, want exactly the projectId equality", clause)
		}
	})
}

// TestApply_ComposesUnderAndBesideAnExistingOr pins the generic composition
// rule. The tolerant project clause is itself an $or, so it must not overwrite
// an unrelated disjunction already present on the caller's filter.
func TestApply_ComposesUnderAndBesideAnExistingOr(t *testing.T) {
	organisation := primitive.NewObjectID()
	defaultProject := models.DefaultProjectId(organisation)
	resourceScope := []bson.M{{"siteId": "site-1"}, {"groupId": "group-1"}}

	filter := Apply(bson.M{
		"$or":            resourceScope,
		"organisationId": organisation.Hex(),
	}, organisation.Hex(), &defaultProject)

	if got, ok := filter["$or"].([]bson.M); !ok || len(got) != 2 {
		t.Errorf("$or = %#v, want the device scope untouched", filter["$or"])
	}
	if filter["organisationId"] != organisation.Hex() {
		t.Errorf("organisationId = %v, want %v", filter["organisationId"], organisation.Hex())
	}
	if _, merged := filter["projectId"]; merged {
		t.Error("the project clause must not be merged as a top-level projectId key")
	}
	and, ok := filter["$and"].([]bson.M)
	if !ok || len(and) != 1 {
		t.Fatalf("$and = %#v, want the project clause nested under it", filter["$and"])
	}
	assertTolerantDefaultProjectClause(t, and[0], defaultProject)
}

func assertTolerantDefaultProjectClause(t *testing.T, clause bson.M, projectId primitive.ObjectID) {
	t.Helper()
	condition, ok := clause["projectId"].(bson.M)
	if !ok {
		t.Fatalf("project clause = %#v, want a projectId condition", clause)
	}
	values, ok := condition["$in"].(bson.A)
	if !ok || len(values) != 2 || values[0] != projectId || values[1] != nil {
		t.Fatalf("projectId condition = %#v, want $in [%v, nil]", condition, projectId)
	}
}

// TestApply_AccumulatesClauses checks that a second Apply on the same filter
// appends rather than replacing, so $and stays a safe home for the axis.
func TestApply_AccumulatesClauses(t *testing.T) {
	organisation := primitive.NewObjectID()
	projectId := primitive.NewObjectID()

	filter := Apply(bson.M{}, organisation.Hex(), &projectId)
	filter = Apply(filter, organisation.Hex(), &projectId)

	if and, ok := filter["$and"].([]bson.M); !ok || len(and) != 2 {
		t.Errorf("$and = %#v, want both clauses", filter["$and"])
	}
}

// TestApply_LeavesFilterUntouchedWithoutAProject checks the no-op path: a caller
// that has no project keeps exactly the filter it built, with no empty $and.
func TestApply_LeavesFilterUntouchedWithoutAProject(t *testing.T) {
	organisationId := primitive.NewObjectID().Hex()
	filter := Apply(bson.M{"organisationId": organisationId}, organisationId, nil)

	if len(filter) != 1 || filter["organisationId"] != organisationId {
		t.Errorf("filter = %#v, want only the organisation clause", filter)
	}
}
