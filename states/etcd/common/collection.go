package common

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
)

var (

	// CollectionMetaPrefix is prefix for rootcoord collection meta.
	CollectionMetaPrefix = path.Join(RCPrefix, CollectionPrefix)
	// DBCollectionMetaPrefix is prefix for rootcoord database collection meta
	DBCollectionMetaPrefix = path.Join(RCPrefix, DBPrefix, CollectionInfoPrefix) // `root-coord/database/collection-info`
	// FieldMetaPrefix is prefix for rootcoord collection fields meta
	FieldMetaPrefix = `root-coord/fields`
	// StructArrayFieldMetaPrefix is prefix for rootcoord collection struct array fields meta.
	StructArrayFieldMetaPrefix = `root-coord/struct-array-fields`
	// FunctionMetaPrefix is prefix for rootcoord function meta
	FunctionMetaPrefix = `root-coord/functions`
	// CollectionLoadPrefix is prefix for querycoord collection loaded in milvus v2.1.x
	CollectionLoadPrefix = "queryCoord-collectionMeta"
	// CollectionLoadPrefixV2 is prefix for querycoord collection loaded in milvus v2.2.x
	CollectionLoadPrefixV2      = "querycoord-collection-loadinfo"
	PartitionLoadedPrefixLegacy = "queryCoord-partitionMeta"

	// CompactionTaskPrefix = "datacoord-meta/compaction-task"
)

var (
	// ErrCollectionDropped sample error for collection dropped.
	ErrCollectionDropped = errors.New("collection dropped")
	// ErrCollectionNotFound sample error for collection not found.
	ErrCollectionNotFound = errors.New("collection not found")
	// CollectionTombstone is the special mark for collection dropped.
	CollectionTombstone = []byte{0xE2, 0x9B, 0xBC}
)

const DefaultListCollectionsBulkEnrichmentThreshold = 1

var (
	collectionMetaKeySpec = MetaKeySpec{
		Prefix: CollectionMetaPrefix,
		Parts:  []MetaKeyPart{KeyCollectionID},
	}
	dbCollectionMetaKeySpec = MetaKeySpec{
		Prefix: DBCollectionMetaPrefix,
		Parts:  []MetaKeyPart{KeyDatabaseID, KeyCollectionID},
	}
)

type CollectionSelector struct {
	DatabaseID     int64
	UseDatabaseID  bool
	CollectionID   int64
	CollectionName string
	Filters        []PostFilter[models.Collection]

	BulkEnrichmentThreshold *int
}

func (s CollectionSelector) legacyMetaKeyHints() MetaKeyHints {
	return NewMetaKeyHints().WithInt64(KeyCollectionID, s.CollectionID)
}

func (s CollectionSelector) dbMetaKeyHints() MetaKeyHints {
	return NewMetaKeyHints().
		WithInt64If(KeyDatabaseID, s.DatabaseID, s.UseDatabaseID).
		WithInt64(KeyCollectionID, s.CollectionID)
}

func (s CollectionSelector) Match(collection *models.Collection) bool {
	proto := collection.GetProto()
	if s.UseDatabaseID && proto.GetDbId() != s.DatabaseID {
		return false
	}
	if s.CollectionID > 0 && proto.GetID() != s.CollectionID {
		return false
	}
	if s.CollectionName != "" {
		schema := proto.GetSchema()
		if schema == nil || schema.GetName() != s.CollectionName {
			return false
		}
	}
	for _, filter := range s.Filters {
		if !filter.Match(collection) {
			return false
		}
	}
	return true
}

func (s CollectionSelector) enrichmentBulkThreshold() int {
	if s.BulkEnrichmentThreshold == nil {
		return DefaultListCollectionsBulkEnrichmentThreshold
	}
	return *s.BulkEnrichmentThreshold
}

func ListCollections(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(*models.Collection) bool) ([]*models.Collection, error) {
	return ListCollectionsBy(ctx, cli, basePath, CollectionSelector{Filters: wrapPostFilters(filters)})
}

func ListCollectionsBy(ctx context.Context, cli kv.MetaKV, basePath string, selector CollectionSelector) ([]*models.Collection, error) {
	results, err := ListCollectionWithoutFieldsBy(ctx, cli, basePath, selector)
	if err != nil {
		return nil, err
	}

	if len(results) > selector.enrichmentBulkThreshold() {
		if err := enrichCollectionsBulk(ctx, cli, basePath, results); err != nil {
			return nil, err
		}
	} else {
		enrichCollectionsIndividually(ctx, cli, basePath, results)
	}

	return results, nil
}

func enrichCollectionsIndividually(ctx context.Context, cli kv.MetaKV, basePath string, collections []*models.Collection) {
	for _, collection := range collections {
		collection.GetProto().GetSchema().Fields, _ = getCollectionFields(ctx, cli, basePath, collection.GetProto().GetID())
		collection.GetProto().GetSchema().StructArrayFields, _ = getCollectionStructFields(ctx, cli, basePath, collection.GetProto().GetID())
		collection.Functions, _ = ListCollectionFunctions(ctx, cli, basePath, collection.GetProto().GetID())
	}
}

func enrichCollectionsBulk(ctx context.Context, cli kv.MetaKV, basePath string, collections []*models.Collection) error {
	wanted := make(map[int64]struct{}, len(collections))
	for _, collection := range collections {
		wanted[collection.GetProto().GetID()] = struct{}{}
	}

	fields, err := listFieldSchemasByCollectionID(ctx, cli, ensureMetaPrefix(path.Join(basePath, FieldMetaPrefix)), wanted)
	if err != nil {
		return err
	}
	structFields, err := listStructArrayFieldSchemasByCollectionID(ctx, cli, ensureMetaPrefix(path.Join(basePath, StructArrayFieldMetaPrefix)), wanted)
	if err != nil {
		return err
	}
	functions, err := listFunctionsByCollectionID(ctx, cli, ensureMetaPrefix(path.Join(basePath, FunctionMetaPrefix)), wanted)
	if err != nil {
		return err
	}

	for _, collection := range collections {
		collectionID := collection.GetProto().GetID()
		collection.GetProto().GetSchema().Fields = fields[collectionID]
		collection.GetProto().GetSchema().StructArrayFields = structFields[collectionID]
		collection.Functions = functions[collectionID]
	}
	return nil
}

func ListCollectionWithoutFields(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(*models.Collection) bool) ([]*models.Collection, error) {
	return ListCollectionWithoutFieldsBy(ctx, cli, basePath, CollectionSelector{Filters: wrapPostFilters(filters)})
}

func ListCollectionWithoutFieldsBy(ctx context.Context, cli kv.MetaKV, basePath string, selector CollectionSelector) ([]*models.Collection, error) {
	var results []*models.Collection
	scans := []struct {
		spec  MetaKeySpec
		hints MetaKeyHints
	}{
		{spec: collectionMetaKeySpec, hints: selector.legacyMetaKeyHints()},
		{spec: dbCollectionMetaKeySpec, hints: selector.dbMetaKeyHints()},
	}
	for _, scan := range scans {
		collections, err := ListObj2ModelsBySpec(ctx, cli, basePath, scan.spec, scan.hints, models.NewCollection, selector)
		if err != nil {
			return nil, err
		}
		results = append(results, collections...)
	}

	return results, nil
}

// GetCollectionByIDVersion retruns collection info from etcd with provided version & id.
func GetCollectionByIDVersion(ctx context.Context, cli kv.MetaKV, basePath string, collID int64) (*models.Collection, error) {
	colls, err := ListCollectionsBy(ctx, cli, basePath, CollectionSelector{CollectionID: collID})
	if err != nil {
		return nil, err
	}

	if len(colls) == 0 {
		return nil, errors.Wrap(ErrCollectionNotFound, fmt.Sprintf("collection %d not found", collID))
	}

	return colls[0], nil
}

func getCollectionFields(ctx context.Context, cli kv.MetaKV, basePath string, collID int64) ([]*schemapb.FieldSchema, error) {
	fields, _, err := ListProtoObjects[schemapb.FieldSchema](ctx, cli, path.Join(basePath, fmt.Sprintf("root-coord/fields/%d", collID)))
	return fields, err
}

func getCollectionStructFields(ctx context.Context, cli kv.MetaKV, basePath string, collID int64) ([]*schemapb.StructArrayFieldSchema, error) {
	fields, _, err := ListProtoObjects[schemapb.StructArrayFieldSchema](ctx, cli, path.Join(basePath, fmt.Sprintf("%s/%d", StructArrayFieldMetaPrefix, collID)))
	return fields, err
}

func listFieldSchemasByCollectionID(ctx context.Context, cli kv.MetaKV, prefix string, wanted map[int64]struct{}) (map[int64][]*schemapb.FieldSchema, error) {
	keys, vals, err := cli.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("listFieldSchemasByCollectionID: keys (%d) and values (%d) length mismatch", len(keys), len(vals))
	}

	fields := make(map[int64][]*schemapb.FieldSchema)
	for i, key := range keys {
		collectionID, err := PathPartInt64(key, -2)
		if err != nil {
			continue
		}
		if _, ok := wanted[collectionID]; !ok {
			continue
		}

		field := &schemapb.FieldSchema{}
		if err := proto.Unmarshal([]byte(vals[i]), field); err != nil {
			continue
		}
		fields[collectionID] = append(fields[collectionID], field)
	}
	return fields, nil
}

func listStructArrayFieldSchemasByCollectionID(ctx context.Context, cli kv.MetaKV, prefix string, wanted map[int64]struct{}) (map[int64][]*schemapb.StructArrayFieldSchema, error) {
	keys, vals, err := cli.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("listStructArrayFieldSchemasByCollectionID: keys (%d) and values (%d) length mismatch", len(keys), len(vals))
	}

	fields := make(map[int64][]*schemapb.StructArrayFieldSchema)
	for i, key := range keys {
		collectionID, err := PathPartInt64(key, -2)
		if err != nil {
			continue
		}
		if _, ok := wanted[collectionID]; !ok {
			continue
		}

		field := &schemapb.StructArrayFieldSchema{}
		if err := proto.Unmarshal([]byte(vals[i]), field); err != nil {
			continue
		}
		fields[collectionID] = append(fields[collectionID], field)
	}
	return fields, nil
}

func listFunctionsByCollectionID(ctx context.Context, cli kv.MetaKV, prefix string, wanted map[int64]struct{}) (map[int64][]*models.Function, error) {
	keys, vals, err := cli.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(vals) {
		return nil, fmt.Errorf("listFunctionsByCollectionID: keys (%d) and values (%d) length mismatch", len(keys), len(vals))
	}

	functions := make(map[int64][]*models.Function)
	for i, key := range keys {
		collectionID, err := PathPartInt64(key, -2)
		if err != nil {
			continue
		}
		if _, ok := wanted[collectionID]; !ok {
			continue
		}

		function := &schemapb.FunctionSchema{}
		if err := proto.Unmarshal([]byte(vals[i]), function); err != nil {
			continue
		}
		functions[collectionID] = append(functions[collectionID], models.NewFunction(function, key))
	}
	return functions, nil
}

func FillFieldSchemaIfEmpty(cli kv.MetaKV, basePath string, collection *etcdpb.CollectionInfo) error {
	if len(collection.GetSchema().GetFields()) == 0 { // fields separated from schema after 2.1.1
		keys, vals, err := cli.LoadWithPrefix(context.TODO(), path.Join(basePath, fmt.Sprintf("root-coord/fields/%d", collection.ID)))
		if err != nil {
			return err
		}
		if len(keys) != len(vals) {
			return fmt.Errorf("error: keys and vals of different size:%d vs %d", len(keys), len(vals))
		}
		for i, key := range keys {
			field := &schemapb.FieldSchema{}
			err := proto.Unmarshal([]byte(vals[i]), field)
			if err != nil {
				fmt.Println("found error field:", key, err.Error())
				continue
			}
			collection.Schema.Fields = append(collection.Schema.Fields, field)
		}
	}

	return nil
}

func FillFieldSchemaIfEmptyV2(cli kv.MetaKV, basePath string, collection *etcdpb.CollectionInfo) error {
	if len(collection.GetSchema().GetFields()) == 0 { // fields separated from schema after 2.1.1
		keys, vals, err := cli.LoadWithPrefix(context.TODO(), fmt.Sprintf("root-coord/fields/%d", collection.ID))
		if err != nil {
			return err
		}
		if len(keys) != len(vals) {
			return fmt.Errorf("error: keys and vals of different size:%d vs %d", len(keys), len(vals))
		}
		for i, key := range keys {
			field := &schemapb.FieldSchema{}
			err := proto.Unmarshal([]byte(vals[i]), field)
			if err != nil {
				fmt.Println("found error field:", key, err.Error())
				continue
			}
			collection.Schema.Fields = append(collection.Schema.Fields, field)
		}
	}

	return nil
}

func UpdateCollection(ctx context.Context, cli kv.MetaKV, key string, fn func(coll *etcdpb.CollectionInfo), dryRun bool) error {
	val, err := cli.Load(ctx, key)
	if err != nil {
		return err
	}

	info := &etcdpb.CollectionInfo{}
	err = proto.Unmarshal([]byte(val), info)
	if err != nil {
		return err
	}

	clone := proto.Clone(info).(*etcdpb.CollectionInfo)
	fn(clone)
	bs, err := proto.Marshal(clone)
	if err != nil {
		return err
	}

	fmt.Println("======dry run======")
	fmt.Println("before alter")
	fmt.Printf("schema:%s\n", info.String())
	fmt.Println()
	fmt.Println("after alter")
	fmt.Printf("schema:%s\n", clone.String())
	if !dryRun {
		return nil
	}

	err = cli.Save(ctx, key, string(bs))
	if err != nil {
		return err
	}
	fmt.Println("Update collection done!")
	return nil
}

func UpdateField(ctx context.Context, cli kv.MetaKV, basePath string, collectionID, fieldID int64, fn func(field *schemapb.FieldSchema) error, dryRun bool) error {
	prefix := path.Join(basePath, SnapshotPrefix, FieldMetaPrefix, strconv.FormatInt(collectionID, 10))
	keys, values, err := cli.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return err
	}

	if len(keys) == 0 {
		return fmt.Errorf("wrong path %s", prefix)
	}

	matchedKey := ""
	var matchedValue []byte
	curTs := int64(0)
	for idx, key := range keys {
		baseName := path.Base(key)
		parts := strings.Split(baseName, "_")
		i, err2 := strconv.Atoi(parts[0])
		if err2 != nil {
			return err2
		}

		if int64(i) == fieldID {
			tsString := parts[1][2:]
			ts, err := strconv.Atoi(tsString)
			if err != nil {
				return nil
			}
			ts2 := int64(ts)
			if ts2 > curTs {
				curTs = ts2
				matchedKey = key
				matchedValue = []byte(values[idx])
			}
		}
	}

	if len(matchedValue) == 0 || len(matchedKey) == 0 {
		return fmt.Errorf("not found field")
	}
	fmt.Println("matchedKey:", matchedKey)
	info := &schemapb.FieldSchema{}
	err = proto.Unmarshal(matchedValue, info)
	if err != nil {
		return err
	}
	fmt.Println("before alter", info)
	err = fn(info)
	if err != nil {
		return err
	}
	bs, err := proto.Marshal(info)
	if err != nil {
		return err
	}

	if dryRun {
		fmt.Printf("try alter field schema:%s\n", info.String())
		return nil
	}
	err = cli.Save(ctx, matchedKey, string(bs))
	if err != nil {
		return err
	}
	fmt.Printf("alter field schema:%s\n", info.String())

	prefixCache := path.Join(basePath, FieldMetaPrefix, strconv.FormatInt(collectionID, 10), strconv.FormatInt(fieldID, 10))
	_, err = cli.Load(ctx, prefixCache)
	if err != nil {
		fmt.Println("no need save to cache")
		return nil
	}
	err = cli.Save(ctx, prefixCache, string(bs))
	if err != nil {
		return err
	}
	fmt.Printf("alter field schema cache :%s\n", info.String())
	return nil
}

func ListCollectionFunctions(ctx context.Context, cli kv.MetaKV, basePath string, collectionID int64) ([]*models.Function, error) {
	prefix := path.Join(basePath, FunctionMetaPrefix, strconv.FormatInt(collectionID, 10)) + "/"
	return ListObj2Models(ctx, cli, prefix, models.NewFunction)
}
