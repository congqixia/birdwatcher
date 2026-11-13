package repair

import (
	"context"
	"fmt"
	"slices"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	milvuscommon "github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// LoadFieldsParam is the parameter of `repair load-fields`.
// It backfills the LoadFields field of the querycoord collection load info
// (etcdpb/querypb.CollectionLoadInfo.LoadFields) with the complete set of
// user-defined field ids of the collection schema, so that fields added after
// the collection was loaded (which leave the previous LoadFields snapshot
// stale, see milvus-io/milvus#55304) are picked up by the preload hints.
// The hidden system columns RowID (0) and Timestamp (1) are never included,
// and fields explicitly configured with field.skipLoad are excluded to mirror
// Milvus load-time semantics. QueryCoord caches the load info in memory, so
// restart it (or reload the collection) after applying for the change to take
// effect.
type LoadFieldsParam struct {
	framework.ExecutionParam `use:"repair load-fields" desc:"backfill the querycoord collection load info LoadFields with the full collection schema field ids (excluding system columns RowID=0 and Timestamp=1 and fields configured with field.skipLoad), so fields added after load are included in the preload hints; QueryCoord caches the load info in memory, restart it or reload the collection afterward for the change to take effect"`
	CollectionID             int64 `name:"collectionID" default:"0" desc:"collection ID to repair (required)"`
}

// LoadFieldsCommand returns the `repair load-fields` command.
func (c *ComponentRepair) LoadFieldsCommand(ctx context.Context, p *LoadFieldsParam) error {
	if p.CollectionID <= 0 {
		return errors.New("collectionID must be specified")
	}

	// 1. Fetch the collection meta with the full field schema.
	//    Since 2.1.1 the field schemas are stored separately from the
	//    collection-info key, so the lookup enriches the schema from
	//    root-coord/fields/{collectionID}; this is what exposes fields that
	//    were added after the collection was loaded.
	collection, err := common.GetCollectionByIDVersion(ctx, c.client, c.basePath, p.CollectionID)
	if err != nil {
		return errors.Wrap(err, "get collection")
	}
	schema := collection.GetProto().GetSchema()
	if schema == nil {
		return errors.Errorf("collection %d has empty schema, refuse to write", p.CollectionID)
	}

	loadFields := collectSchemaLoadFields(schema)
	if len(loadFields) == 0 {
		return errors.Errorf("collection %d schema has no loadable user-defined fields", p.CollectionID)
	}

	// 2. Fetch the persisted querycoord collection load info of the collection.
	infos, err := common.ListCollectionLoadedInfo(ctx, c.client, c.basePath, func(info *models.CollectionLoaded) bool {
		return info.GetProto().GetCollectionID() == p.CollectionID
	})
	if err != nil {
		return errors.Wrap(err, "list collection load info")
	}
	if len(infos) == 0 {
		return errors.Wrap(common.ErrCollectionNotFound, fmt.Sprintf("collection %d has no querycoord load info, the collection may not be loaded", p.CollectionID))
	}

	// 3. Plan every write before touching etcd: marshal errors surface before
	//    anything is committed, so a failure cannot leave a partially rewritten
	//    state; the whole plan is printed first, then applied.
	type loadFieldsUpdate struct {
		key   string
		value string
	}
	var updates []loadFieldsUpdate
	for _, loaded := range infos {
		info := loaded.GetProto()
		before := append([]int64(nil), info.GetLoadFields()...)
		fmt.Printf("[querycoord collection load info] %s\n", loaded.Key())
		fmt.Printf("  collectionID: %d  loadFields: %v -> %v\n", info.GetCollectionID(), before, loadFields)

		clone := proto.Clone(info).(*querypb.CollectionLoadInfo)
		clone.LoadFields = loadFields
		bs, err := proto.Marshal(clone)
		if err != nil {
			return errors.Wrap(err, "marshal collection load info")
		}
		updates = append(updates, loadFieldsUpdate{key: loaded.Key(), value: string(bs)})
	}

	if p.IsDryRun() {
		fmt.Println("dry run, nothing written. re-run with --run to apply.")
		return nil
	}
	for i, update := range updates {
		if err := c.client.Save(ctx, update.key, update.value); err != nil {
			return errors.Wrapf(err, "save collection load info at %s (%d/%d records committed before failure; re-run to finish)", update.key, i, len(updates))
		}
	}
	fmt.Println("collection load fields repaired successfully. restart QueryCoord or reload the collection so the corrected LoadFields is picked up from etcd.")
	return nil
}

// collectSchemaLoadFields returns every user-defined field id of the
// collection schema, excluding the hidden system columns RowID (0) and
// Timestamp (1) and any field explicitly configured with field.skipLoad (the
// same filter Milvus applies at load time via common.GetCollectionLoadFields,
// where an unparseable skipLoad config is ignored and the field is loaded).
// Sub-fields of struct-array fields are included because they carry their own
// field ids in the load hints. The result is de-duplicated and sorted
// ascending for stable output.
func collectSchemaLoadFields(schema *schemapb.CollectionSchema) []int64 {
	seen := make(map[int64]struct{})
	collect := func(fields []*schemapb.FieldSchema) {
		for _, field := range fields {
			// user-defined field ids start at common.StartOfUserFieldID (100),
			// so this excludes the system columns 0 and 1 as well as any other
			// hidden system field.
			if milvuscommon.IsSystemField(field.GetFieldID()) {
				continue
			}
			loaded, err := milvuscommon.ShouldFieldBeLoaded(field.GetTypeParams())
			if err != nil {
				// mirror GetCollectionLoadFields: an unparseable skipLoad config
				// is ignored and the field is loaded.
				loaded = true
			}
			if !loaded {
				continue
			}
			seen[field.GetFieldID()] = struct{}{}
		}
	}
	collect(schema.GetFields())
	for _, structField := range schema.GetStructArrayFields() {
		collect(structField.GetFields())
	}

	ids := make([]int64, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
