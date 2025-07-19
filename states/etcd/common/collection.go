package common

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

var (

	// CollectionMetaPrefix is prefix for rootcoord collection meta.
	CollectionMetaPrefix = path.Join(RCPrefix, CollectionPrefix)
	// DBCollectionMetaPrefix is prefix for rootcoord database collection meta
	DBCollectionMetaPrefix = path.Join(RCPrefix, DBPrefix, CollectionInfoPrefix) // `root-coord/database/collection-info`
	// FieldMetaPrefix is prefix for rootcoord collection fields meta
	FieldMetaPrefix = `root-coord/fields`
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

func ListCollections(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(*models.Collection) bool) ([]*models.Collection, error) {
	prefixes := []string{
		path.Join(basePath, CollectionMetaPrefix),
		path.Join(basePath, DBCollectionMetaPrefix),
	}
	var results []*models.Collection
	for _, prefix := range prefixes {
		collections, err := ListObj2Models(ctx, cli, prefix, models.NewCollection, filters...)
		if err != nil {
			return nil, err
		}
		results = append(results, collections...)
	}

	results = lo.Map(results, func(collection *models.Collection, _ int) *models.Collection {
		collection.GetProto().GetSchema().Fields, _ = getCollectionFields(ctx, cli, basePath, collection.GetProto().GetID())
		collection.Functions, _ = ListCollectionFunctions(ctx, cli, basePath, collection.GetProto().GetID())
		return collection
	})

	return results, nil
}

func ListCollectionWithoutFields(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(*models.Collection) bool) ([]*models.Collection, error) {
	prefixes := []string{
		path.Join(basePath, CollectionMetaPrefix),
		path.Join(basePath, DBCollectionMetaPrefix),
	}
	var results []*models.Collection
	for _, prefix := range prefixes {
		collections, err := ListObj2Models(ctx, cli, prefix, models.NewCollection, filters...)
		if err != nil {
			return nil, err
		}
		results = append(results, collections...)
	}

	return results, nil
}

// GetCollectionByIDVersion retruns collection info from etcd with provided version & id.
func GetCollectionByIDVersion(ctx context.Context, cli kv.MetaKV, basePath string, collID int64) (*models.Collection, error) {
	colls, err := ListCollections(ctx, cli, basePath, func(c *models.Collection) bool {
		return c.GetProto().GetID() == collID
	})
	if err != nil {
		return nil, err
	}

	if len(colls) == 0 {
		return nil, errors.Newf("collection with id %d not found", collID)
	}

	return colls[0], nil
}

func getCollectionFields(ctx context.Context, cli kv.MetaKV, basePath string, collID int64) ([]*schemapb.FieldSchema, error) {
	fields, _, err := ListProtoObjects[schemapb.FieldSchema](ctx, cli, path.Join(basePath, fmt.Sprintf("root-coord/fields/%d", collID)))
	return fields, err
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

func UpdateField(ctx context.Context, cli kv.MetaKV, basePath string, collectionID, fieldID int64, fn func(field *schemapb.FieldSchema), dryRun bool) error {
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
	fn(info)
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
