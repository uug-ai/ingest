package markers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uug-ai/models/pkg/models"
	"github.com/uug-ai/trace/pkg/opentelemetry"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	MARKERS_COLLECTION                    = "markers"
	MARKER_OPTIONS_COLLECTION             = "marker_options"
	MARKER_OPTION_RANGES_COLLECTION       = "marker_option_ranges"
	MARKER_TAG_OPTIONS_COLLECTION         = "marker_tag_options"
	MARKER_TAG_OPTION_RANGES_COLLECTION   = "marker_tag_option_ranges"
	MARKER_EVENT_OPTIONS_COLLECTION       = "marker_event_options"
	MARKER_EVENT_OPTION_RANGES_COLLECTION = "marker_event_option_ranges"
	MARKER_CATEGORY_OPTIONS_COLLECTION    = "marker_category_options"
	MEDIA_COLLECTION                      = "media"

	DatabaseName = "Kerberos"
	TIMEOUT      = 10 * time.Second
)

// AddMarkerToMongodb inserts a fresh marker (a new _id every call) together with
// its denormalised option/range/category projections and media tagging. Two
// calls with the same content create two distinct markers; use it for authoring
// doors where that is the intent (the media UI, analysers, alert pipelines).
func AddMarkerToMongodb(ctxTracer context.Context, tracer *opentelemetry.Tracer, client *mongo.Client, marker models.Marker, mediaIds ...string) (models.Marker, error) {
	return addMarker(ctxTracer, tracer, client, marker, false, mediaIds...)
}

// UpsertMarkerToMongodb upserts the marker by its stable identity
// (organisationId, deviceId, name, startTimestamp) and writes its denormalised
// range documents idempotently, so an at-least-once redelivery or a re-analysis
// of the same recording refreshes the marker rather than duplicating it. It is
// the writer the ingest core's marker sink uses.
func UpsertMarkerToMongodb(ctxTracer context.Context, tracer *opentelemetry.Tracer, client *mongo.Client, marker models.Marker, mediaIds ...string) (models.Marker, error) {
	return addMarker(ctxTracer, tracer, client, marker, true, mediaIds...)
}

// addMarker is the shared writer behind the insert and upsert entry points. The
// only behaviour that differs between the two modes is how the marker document
// and its *_ranges documents are written: the insert mode appends fresh
// documents, the idempotent mode keys them so a replay refreshes in place. The
// option/tag/event/category lookups and the media tagging are already idempotent
// keyed upserts in both modes.
func addMarker(ctxTracer context.Context, tracer *opentelemetry.Tracer, client *mongo.Client, marker models.Marker, idempotent bool, mediaIds ...string) (models.Marker, error) {

	// The tracer is optional: the ingest core has no per-result tracer to thread,
	// and the marker write was untraced before this writer existed. Skip the span
	// when none is supplied rather than forcing every caller to own one.
	ctx := ctxTracer
	if tracer != nil {
		spanCtx, span := tracer.CreateSpan(ctxTracer, map[string]string{})
		defer span.End()
		ctx = spanCtx
	}

	ctx, cancel := context.WithTimeout(ctx, TIMEOUT)
	defer cancel()

	// Open markers collection
	db := client.Database(DatabaseName)
	c := db.Collection(MARKERS_COLLECTION)

	if idempotent {
		// Upsert by stable identity so a redelivery refreshes the marker instead
		// of inserting a duplicate. _id is owned by the first insert and never
		// reassigned, so anything that linked to the marker survives the replay.
		set, err := markerSetDoc(marker)
		if err != nil {
			return models.Marker{}, err
		}
		filter := bson.M{
			"organisationId": marker.OrganisationId,
			"deviceId":       marker.DeviceId,
			"name":           marker.Name,
			"startTimestamp": marker.StartTimestamp,
		}
		update := bson.M{
			"$set":         set,
			"$setOnInsert": bson.M{"_id": primitive.NewObjectID()},
		}
		res, err := c.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
		if err != nil {
			return models.Marker{}, err
		}
		// Resolve the marker's _id for the returned value: the upserted id on a
		// fresh insert, otherwise the existing document's id.
		if oid, ok := res.UpsertedID.(primitive.ObjectID); ok {
			marker.Id = oid
		} else {
			var existing struct {
				Id primitive.ObjectID `bson:"_id"`
			}
			if err := c.FindOne(ctx, filter, options.FindOne().SetProjection(bson.M{"_id": 1})).Decode(&existing); err == nil {
				marker.Id = existing.Id
			}
		}
	} else {
		// Generate new ID for the marker
		marker.Id = primitive.NewObjectID()

		res, err := c.InsertOne(ctx, marker)
		if err != nil {
			return models.Marker{}, err
		}

		// Check if the inserted ID is of type primitive.ObjectID
		if res.InsertedID == nil {
			return models.Marker{}, errors.New("Inserted ID is nil")
		}

		if _, ok := res.InsertedID.(primitive.ObjectID); !ok {
			return models.Marker{}, errors.New("Inserted ID is not of type primitive.ObjectID")
		}

		marker.Id = res.InsertedID.(primitive.ObjectID)
	}

	// As part of the marker we also need to insert into some other collections for performance reasons.
	// For example on the media page we have marker options, marker event options, marker tag options, marker category options.

	// Collections for tracking unique entries
	nameSet := make(map[string]struct{})
	tagSet := make(map[string]struct{})
	eventSet := make(map[string]struct{})
	categorySet := make(map[string]struct{})

	// Slices for bulk operations
	var markerOptUpserts []mongo.WriteModel
	var tagOptUpserts []mongo.WriteModel
	var eventOptUpserts []mongo.WriteModel
	var categoryOptUpserts []mongo.WriteModel

	// Slices for range documents
	var markerRangeDocs []any
	var tagRangeDocs []any
	var eventRangeDocs []any

	now := time.Now().Unix()

	// marker option upsert
	if marker.Name != "" {
		if _, exists := nameSet[marker.Name]; !exists {
			nameSet[marker.Name] = struct{}{}
			categoryNamesList := make([]string, 0)
			for _, cat := range marker.Categories {
				if cat.Name != "" {
					categoryNamesList = append(categoryNamesList, cat.Name)
				}
			}
			up := mongo.NewUpdateOneModel()
			up.SetFilter(bson.M{"value": marker.Name, "organisationId": marker.OrganisationId})
			up.SetUpdate(bson.M{
				"$setOnInsert": bson.M{
					"value":          marker.Name,
					"text":           marker.Name,
					"organisationId": marker.OrganisationId,
					"createdAt":      now,
				},
				"$set": bson.M{
					"updatedAt": now,
				},
				"$addToSet": bson.M{
					"categories": bson.M{"$each": categoryNamesList},
				},
			})
			up.SetUpsert(true)
			markerOptUpserts = append(markerOptUpserts, up)
		}
		markerRangeDocs = append(markerRangeDocs, bson.M{
			"value":          marker.Name,
			"text":           marker.Name,
			"organisationId": marker.OrganisationId,
			"start":          marker.StartTimestamp,
			"end":            marker.EndTimestamp,
			"deviceId":       marker.DeviceId,
			"groupId":        marker.GroupId,
			"createdAt":      now,
		})
	}

	// tags
	for _, tag := range marker.Tags {
		if tag.Name == "" {
			continue
		}
		if _, exists := tagSet[tag.Name]; !exists {
			tagSet[tag.Name] = struct{}{}
			up := mongo.NewUpdateOneModel()
			up.SetFilter(bson.M{"value": tag.Name, "organisationId": marker.OrganisationId})
			up.SetUpdate(bson.M{
				"$setOnInsert": bson.M{
					"value":          tag.Name,
					"text":           tag.Name,
					"organisationId": marker.OrganisationId,
					"createdAt":      now,
				},
				"$set": bson.M{
					"updatedAt": now,
				},
			})
			up.SetUpsert(true)
			tagOptUpserts = append(tagOptUpserts, up)
		}
		tagRangeDocs = append(tagRangeDocs, bson.M{
			"value":          tag.Name,
			"text":           tag.Name,
			"organisationId": marker.OrganisationId,
			"start":          marker.StartTimestamp,
			"end":            marker.EndTimestamp,
			"deviceId":       marker.DeviceId,
			"groupId":        marker.GroupId,
			"createdAt":      now,
		})
	}

	// events
	for _, event := range marker.Events {
		if event.Name == "" {
			continue
		}
		if _, exists := eventSet[event.Name]; !exists {
			eventSet[event.Name] = struct{}{}
			up := mongo.NewUpdateOneModel()
			up.SetFilter(bson.M{"value": event.Name, "organisationId": marker.OrganisationId})
			up.SetUpdate(bson.M{
				"$setOnInsert": bson.M{
					"value":          event.Name,
					"text":           event.Name,
					"organisationId": marker.OrganisationId,
					"createdAt":      now,
				},
				"$set": bson.M{
					"updatedAt": now,
				},
			})
			up.SetUpsert(true)
			eventOptUpserts = append(eventOptUpserts, up)
		}
		eventRangeDocs = append(eventRangeDocs, bson.M{
			"value":          event.Name,
			"text":           event.Name,
			"organisationId": marker.OrganisationId,
			"start":          event.StartTimestamp,
			"end":            event.EndTimestamp,
			"deviceId":       marker.DeviceId,
			"groupId":        marker.GroupId,
			"createdAt":      now,
			"updatedAt":      now,
		})
	}

	// categories
	for _, category := range marker.Categories {
		if category.Name == "" {
			continue
		}
		if _, exists := categorySet[category.Name]; !exists {
			categorySet[category.Name] = struct{}{}
			up := mongo.NewUpdateOneModel()
			up.SetFilter(bson.M{"value": category.Name, "organisationId": marker.OrganisationId})
			up.SetUpdate(bson.M{
				"$setOnInsert": bson.M{
					"value":          category.Name,
					"text":           category.Name,
					"organisationId": marker.OrganisationId,
					"createdAt":      now,
				},
				"$set": bson.M{
					"updatedAt": now,
				},
			})
			up.SetUpsert(true)
			categoryOptUpserts = append(categoryOptUpserts, up)
		}
	}

	// Execute bulk operations for marker options
	if len(markerOptUpserts) > 0 {
		markerOptCol := db.Collection(MARKER_OPTIONS_COLLECTION)
		if _, err := markerOptCol.BulkWrite(ctx, markerOptUpserts); err != nil {
			return marker, fmt.Errorf("failed to upsert marker options: %w", err)
		}
	}

	// Write marker option ranges
	if err := writeRanges(ctx, db.Collection(MARKER_OPTION_RANGES_COLLECTION), markerRangeDocs, idempotent); err != nil {
		return marker, fmt.Errorf("failed to write marker ranges: %w", err)
	}

	// Execute bulk operations for tag options
	if len(tagOptUpserts) > 0 {
		tagOptCol := db.Collection(MARKER_TAG_OPTIONS_COLLECTION)
		if _, err := tagOptCol.BulkWrite(ctx, tagOptUpserts); err != nil {
			return marker, fmt.Errorf("failed to upsert tag options: %w", err)
		}
	}

	// Write tag option ranges
	if err := writeRanges(ctx, db.Collection(MARKER_TAG_OPTION_RANGES_COLLECTION), tagRangeDocs, idempotent); err != nil {
		return marker, fmt.Errorf("failed to write tag ranges: %w", err)
	}

	// Execute bulk operations for event options
	if len(eventOptUpserts) > 0 {
		eventOptCol := db.Collection(MARKER_EVENT_OPTIONS_COLLECTION)
		if _, err := eventOptCol.BulkWrite(ctx, eventOptUpserts); err != nil {
			return marker, fmt.Errorf("failed to upsert event options: %w", err)
		}
	}

	// Write event option ranges
	if err := writeRanges(ctx, db.Collection(MARKER_EVENT_OPTION_RANGES_COLLECTION), eventRangeDocs, idempotent); err != nil {
		return marker, fmt.Errorf("failed to write event ranges: %w", err)
	}

	// Execute bulk operations for category options
	if len(categoryOptUpserts) > 0 {
		categoryOptCol := db.Collection(MARKER_CATEGORY_OPTIONS_COLLECTION)
		if _, err := categoryOptCol.BulkWrite(ctx, categoryOptUpserts); err != nil {
			return marker, fmt.Errorf("failed to upsert category options: %w", err)
		}
	}

	// Media tagging: denormalise the marker/tag/event names onto the media docs so
	// the frontend can list them without a join. The recording(s) a marker belongs
	// to are resolved in one of three ways, in priority order:
	//
	//  1. Explicit media keys on the marker (authoritative). The producer knows
	//     exactly which recording(s) the marker was derived from (media.videoFile),
	//     so stamp those recordings directly — scoped to the marker's device and
	//     organisation so a caller can only tag media it owns, and with NO
	//     timestamp guard, since the key is the source of truth (immune to
	//     timing/fps drift).
	//  2. Explicit media _ids (legacy by-id authoring path). Stamp those docs,
	//     still guarded by timestamp overlap.
	//  3. No explicit reference. Fall back to timestamp overlap on the device.
	markerNames, tagNames, eventNames, categoryNames := markerDenormNames(marker)
	updateDoc := bson.M{}
	if len(markerNames) > 0 {
		updateDoc["markerNames"] = bson.M{"$each": markerNames}
	}
	if len(tagNames) > 0 {
		updateDoc["tagNames"] = bson.M{"$each": tagNames}
	}
	if len(eventNames) > 0 {
		updateDoc["eventNames"] = bson.M{"$each": eventNames}
	}
	if len(categoryNames) > 0 {
		updateDoc["categoryNames"] = bson.M{"$each": categoryNames}
	}

	// Combined update: $addToSet keeps the flat name arrays deduped for filtering,
	// while $push appends a per-occurrence markerSummary entry for display. The
	// $slice bounds the array so busy devices can't grow a media doc without limit
	// (keeps the most recent markerSummaryMaxEntries entries).
	update := bson.M{}
	if len(updateDoc) > 0 {
		update["$addToSet"] = updateDoc
	}
	if summary, ok := markerSummaryEntry(marker); ok {
		update["$push"] = bson.M{
			"markerSummary": bson.M{
				"$each":  []bson.M{summary},
				"$slice": -markerSummaryMaxEntries,
			},
		}
	}

	switch {
	case len(marker.MediaKeys) > 0:
		if len(update) > 0 {
			mediaCol := db.Collection(MEDIA_COLLECTION)
			for _, key := range marker.MediaKeys {
				if key == "" {
					continue
				}
				// Authoritative link by recording key (media.videoFile), scoped to
				// the marker's device/organisation. No timestamp guard: the key
				// pins the recording regardless of the marker's absolute timing.
				filter := bson.M{"videoFile": key}
				if marker.DeviceId != "" {
					filter["deviceId"] = marker.DeviceId
				}
				if marker.OrganisationId != "" {
					filter["organisationId"] = marker.OrganisationId
				}
				if _, err := mediaCol.UpdateMany(ctx, filter, update); err != nil {
					return marker, fmt.Errorf("failed to update media by key with marker data: %w", err)
				}
			}
		}

	case len(mediaIds) > 0:
		for _, mediaId := range mediaIds {
			if mediaId == "" {
				continue
			}
			mediaObjectId, err := primitive.ObjectIDFromHex(mediaId)
			if err != nil {
				return marker, fmt.Errorf("invalid mediaId format: %w", err)
			}
			if len(update) > 0 {
				mediaCol := db.Collection(MEDIA_COLLECTION)
				filter := bson.M{
					"_id":            mediaObjectId,
					"startTimestamp": bson.M{"$lte": marker.EndTimestamp},
					"endTimestamp":   bson.M{"$gte": marker.StartTimestamp},
				}
				if _, err := mediaCol.UpdateOne(ctx, filter, update); err != nil {
					return marker, fmt.Errorf("failed to update media with marker data: %w", err)
				}
			}
		}

	default:
		// No explicit reference: media that overlap with the marker in time may
		// still exist. Update those (deviceId + timestamp overlap) so the frontend
		// can list the names without a join.
		if len(update) > 0 {
			mediaCol := db.Collection(MEDIA_COLLECTION)
			filter := bson.M{
				"deviceId":       marker.DeviceId,
				"startTimestamp": bson.M{"$lte": marker.EndTimestamp},
				"endTimestamp":   bson.M{"$gte": marker.StartTimestamp},
			}
			if _, err := mediaCol.UpdateMany(ctx, filter, update); err != nil {
				return marker, fmt.Errorf("failed to update overlapping media with marker data: %w", err)
			}
		}
	}

	return marker, nil
}

// markerDenormNames collects the unique, non-empty marker/tag/event/category names that
// are denormalised onto media documents for frontend listing.
func markerDenormNames(marker models.Marker) (markerNames, tagNames, eventNames, categoryNames []string) {
	if marker.Name != "" {
		markerNames = append(markerNames, marker.Name)
	}
	for _, tag := range marker.Tags {
		if tag.Name != "" {
			tagNames = append(tagNames, tag.Name)
		}
	}
	for _, event := range marker.Events {
		if event.Name != "" {
			eventNames = append(eventNames, event.Name)
		}
	}
	for _, category := range marker.Categories {
		if category.Name != "" {
			categoryNames = append(categoryNames, category.Name)
		}
	}
	return markerNames, tagNames, eventNames, categoryNames
}

// markerSummaryMaxEntries caps the per-media markerSummary array so append-only
// $push growth stays bounded on busy devices (the $slice keeps the most recent
// entries).
const markerSummaryMaxEntries = 200

// markerSummaryEntry builds the per-occurrence markerSummary document for a
// marker, preserving the correlation between the marker's name, its
// category/event/tag names and its time range. The second return is false when
// the marker carries nothing worth recording.
func markerSummaryEntry(marker models.Marker) (bson.M, bool) {
	_, tagNames, eventNames, categoryNames := markerDenormNames(marker)
	entry := bson.M{}
	if marker.Name != "" {
		entry["name"] = marker.Name
	}
	if len(categoryNames) > 0 {
		entry["categoryNames"] = categoryNames
	}
	if len(eventNames) > 0 {
		entry["eventNames"] = eventNames
	}
	if len(tagNames) > 0 {
		entry["tagNames"] = tagNames
	}
	if marker.StartTimestamp != 0 {
		entry["startTimestamp"] = marker.StartTimestamp
	}
	if marker.EndTimestamp != 0 {
		entry["endTimestamp"] = marker.EndTimestamp
	}
	if len(marker.Detections) > 0 {
		entry["detections"] = marker.Detections
	}
	if len(entry) == 0 {
		return nil, false
	}
	return entry, true
}

// markerSetDoc marshals a marker through BSON (so its bson tags / omitempty
// rules decide what is written) and removes _id, yielding the mutable field set
// for an upsert that must not overwrite an existing document's id.
func markerSetDoc(m models.Marker) (bson.M, error) {
	raw, err := bson.Marshal(m)
	if err != nil {
		return nil, err
	}
	var doc bson.M
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	delete(doc, "_id")
	return doc, nil
}

// writeRanges persists the denormalised *_ranges documents. In insert mode it
// appends them (the authoring path, where a marker is written once). In
// idempotent mode it upserts each document keyed by its natural identity
// (value, organisationId, deviceId, start, end) so a redelivery of the same
// marker refreshes the range rather than duplicating it; createdAt is seeded
// once on first insert.
func writeRanges(ctx context.Context, col *mongo.Collection, docs []any, idempotent bool) error {
	if len(docs) == 0 {
		return nil
	}

	if !idempotent {
		_, err := col.InsertMany(ctx, docs)
		return err
	}

	writeModels := make([]mongo.WriteModel, 0, len(docs))
	for _, d := range docs {
		doc, ok := d.(bson.M)
		if !ok {
			// A range we cannot key falls back to an insert so it is never dropped.
			writeModels = append(writeModels, mongo.NewInsertOneModel().SetDocument(d))
			continue
		}

		filter := bson.M{
			"value":          doc["value"],
			"organisationId": doc["organisationId"],
			"deviceId":       doc["deviceId"],
			"start":          doc["start"],
			"end":            doc["end"],
		}

		set := bson.M{}
		setOnInsert := bson.M{}
		for k, v := range doc {
			switch k {
			case "value", "organisationId", "deviceId", "start", "end":
				// Part of the identity filter; Mongo seeds these from the filter on
				// insert, so they need not be repeated in the update.
				continue
			case "createdAt":
				setOnInsert[k] = v
			default:
				set[k] = v
			}
		}

		update := bson.M{}
		if len(set) > 0 {
			update["$set"] = set
		}
		if len(setOnInsert) > 0 {
			update["$setOnInsert"] = setOnInsert
		}

		writeModels = append(writeModels,
			mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
	}

	_, err := col.BulkWrite(ctx, writeModels)
	return err
}
