// Package projectfilter builds the project predicate every ingest sink narrows
// its queries with.
//
// It exists so the marker, detection and media writers share one definition of
// "documents in this project" rather than three copies that can drift apart —
// and, more importantly, one definition of how that predicate behaves against
// documents written before the project field existed.
//
// The package is infra-free beyond bson: it performs no I/O and imports no sink,
// so it can be used from every writer without creating a dependency cycle.
package projectfilter

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Predicate returns the value to place under a "projectId" key to narrow a query
// to one project, or nil when there is no project to scope to (the caller then
// omits the key and the query stays organisation-wide).
//
// The predicate is deliberately tolerant. Every document written before
// projectId existed has no such field, and in MongoDB a missing field matches
// null — so the null arm of the $in is what keeps a pre-rollout marker, range or
// media reachable. Without it an upsert keyed on this predicate would fail to
// match its own earlier document and insert a duplicate instead of refreshing
// it, and the marker→media link would silently tag nothing. Because the writers
// pair this predicate with a $set that carries projectId, a matched legacy
// document is back-filled by the same operation that found it.
//
// The null arm is therefore a rollout affordance, not the intended end state:
// once every document carries projectId, drop it so the predicate isolates
// strictly. Until then it costs nothing — during the single-project rollout
// models.DefaultProjectId(org) == org, so the project axis is redundant with the
// organisation predicate it sits beside rather than weaker than it.
func Predicate(projectId *primitive.ObjectID) bson.M {
	if projectId == nil || projectId.IsZero() {
		return nil
	}
	return bson.M{"$in": bson.A{*projectId, nil}}
}

// Apply sets the project predicate on filter when there is one to apply, and
// leaves filter untouched otherwise. It writes a single top-level "projectId"
// key so it never collides with an $or a caller already uses for another axis
// (the marker writers scope the device that way).
func Apply(filter bson.M, projectId *primitive.ObjectID) bson.M {
	if predicate := Predicate(projectId); predicate != nil {
		filter["projectId"] = predicate
	}
	return filter
}
