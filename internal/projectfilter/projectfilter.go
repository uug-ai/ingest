// Package projectfilter narrows an ingest sink's queries to one project.
//
// It owns no rule of its own. The project predicate is defined once, in the
// models module, next to the writer that stamps the field
// (models.ResolveProjectId / models.ProjectScopeFilter) — a reader must match
// exactly what a writer stamped, and a predicate that drifts from it returns
// zero documents rather than an error, so the two are deliberately co-located
// there. This package is only the adapter that lets ingest's writers, which
// hold an optional *primitive.ObjectID and an organisation string, call it and
// compose the result into a filter they are already building.
//
// Nothing here reimplements the predicate. If the project rule changes, it
// changes in models and every consumer follows on the next bump.
package projectfilter

import (
	"github.com/uug-ai/models/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Clause returns the match expression that narrows a query to projectId inside
// organisationId, or nil when there is nothing to narrow by (no project, a zero
// project, or an organisation that is not an ObjectID — in which case the query
// stays organisation-wide rather than becoming unmatchable).
//
// The shape depends on the project, and that is the whole point:
// models.ProjectScopeFilter returns strict equality for a real project and only
// relaxes to "or unstamped" for the organisation's default project. Ingest used
// to relax unconditionally. That reads identically today — during the hidden
// rollout every project IS its organisation's default — but the moment a second
// project exists, an unconditional null arm makes that project's queries match
// every unstamped document in the organisation. Hence the organisation id is
// now required here: without it the caller cannot tell a default project from a
// real one, and so cannot tell which of the two shapes is correct.
func Clause(organisationId string, projectId *primitive.ObjectID) bson.M {
	if projectId == nil {
		return nil
	}
	return models.ProjectScopeFilter(organisationId, *projectId)
}

// Apply composes the project clause into filter, and leaves filter untouched
// when there is no project to scope to.
//
// It composes under $and rather than merging the clause's keys into filter,
// because the tolerant form of the predicate is ITSELF an $or and several
// callers already spend their top-level $or on another axis (the marker writers
// scope the device that way). Merging would have one $or overwrite the other,
// and the resulting query would quietly stop matching on device — no error, no
// duplicate-key, just a marker that tags nothing. $and nests both, so each axis
// keeps its own operator.
//
// $and is owned by this package: callers keep their other clauses under their
// own keys and let this one accumulate here.
func Apply(filter bson.M, organisationId string, projectId *primitive.ObjectID) bson.M {
	clause := Clause(organisationId, projectId)
	if clause == nil {
		return filter
	}
	existing, _ := filter["$and"].([]bson.M)
	filter["$and"] = append(existing, clause)
	return filter
}
