package repair

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// fakeMetaKV is an in-memory MetaKV used to verify the schema version repair
// command reads from and writes to the expected etcd keys.
type fakeMetaKV struct {
	kv.MetaKV
	data map[string]string
}

func (f *fakeMetaKV) Load(ctx context.Context, key string, opts ...kv.LoadOption) (string, error) {
	v, ok := f.data[key]
	if !ok {
		return "", kv.ErrKeyNotFound
	}
	return v, nil
}

func (f *fakeMetaKV) LoadWithPrefix(ctx context.Context, key string, opts ...kv.LoadOption) ([]string, []string, error) {
	var keys, vals []string
	for k, v := range f.data {
		if strings.HasPrefix(k, key) {
			keys = append(keys, k)
			vals = append(vals, v)
		}
	}
	return keys, vals, nil
}

func (f *fakeMetaKV) Save(ctx context.Context, key, value string) error {
	f.data[key] = value
	return nil
}

const (
	testBasePath   = "by-dev/meta"
	testCollection = int64(1)
	testPChannel   = "by-dev-rootcoord-dml_0"
	testVChannel   = "by-dev-rootcoord-dml_0_1v0"
)

func mustMarshal(t *testing.T, msg proto.Message) string {
	t.Helper()
	bs, err := proto.Marshal(msg)
	require.NoError(t, err)
	return string(bs)
}

// seedSchemaVersionTestKV populates a fake MetaKV with one collection (legacy
// path) owning one pchannel with two schema snapshots, plus unrelated noise
// data (another collection and another pchannel) that must stay untouched.
func seedSchemaVersionTestKV(t *testing.T) *fakeMetaKV {
	t.Helper()
	cli := &fakeMetaKV{data: map[string]string{}}

	// target collection meta (legacy path, no db-id segment).
	coll := &etcdpb.CollectionInfo{
		ID: testCollection,
		Schema: &schemapb.CollectionSchema{
			Name:    "coll_1",
			Version: 3,
		},
	}
	cli.data[testBasePath+"/root-coord/collection/1"] = mustMarshal(t, coll)

	// unrelated collection meta (db path), must not be touched.
	noise := &etcdpb.CollectionInfo{
		ID:   2,
		DbId: 7,
		Schema: &schemapb.CollectionSchema{
			Name:    "coll_2",
			Version: 9,
		},
	}
	cli.data[testBasePath+"/root-coord/database/collection-info/7/2"] = mustMarshal(t, noise)

	// pchannel assignment of the target collection.
	cli.data[testBasePath+"/streamingcoord-meta/pchannel/"+testPChannel] = mustMarshal(t, &streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{Name: testPChannel},
	})

	// vchannel base meta of the target collection.
	cli.data[testBasePath+"/streamingnode-meta/wal/"+testPChannel+"/vchannel/"+testVChannel] = mustMarshal(t, &streamingpb.VChannelMeta{
		Vchannel: testVChannel,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: testCollection,
		},
	})

	// two schema snapshots of the target collection.
	for _, tick := range []uint64{100, 200} {
		cli.data[testBasePath+"/streamingnode-meta/wal/"+testPChannel+"/vchannel/"+testVChannel+"/schema/"+strconv.FormatUint(tick, 10)] = mustMarshal(t, &streamingpb.CollectionSchemaOfVChannel{
			Schema: &schemapb.CollectionSchema{
				Name:    "coll_1",
				Version: int32(tick / 100), // 1 and 2
			},
			CheckpointTimeTick: tick,
		})
	}

	// a dropped schema snapshot that must never be selected as latest.
	cli.data[testBasePath+"/streamingnode-meta/wal/"+testPChannel+"/vchannel/"+testVChannel+"/schema/300"] = mustMarshal(t, &streamingpb.CollectionSchemaOfVChannel{
		Schema: &schemapb.CollectionSchema{
			Name:    "coll_1",
			Version: 3,
		},
		CheckpointTimeTick: 300,
		State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED,
	})

	// unrelated pchannel/schema of another collection, must not be touched.
	noiseVChannel := "by-dev-rootcoord-dml_1_2v0"
	cli.data[testBasePath+"/streamingcoord-meta/pchannel/by-dev-rootcoord-dml_1"] = mustMarshal(t, &streamingpb.PChannelMeta{
		Channel: &streamingpb.PChannelInfo{Name: "by-dev-rootcoord-dml_1"},
	})
	cli.data[testBasePath+"/streamingnode-meta/wal/by-dev-rootcoord-dml_1/vchannel/"+noiseVChannel] = mustMarshal(t, &streamingpb.VChannelMeta{
		Vchannel: noiseVChannel,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 42,
		},
	})
	cli.data[testBasePath+"/streamingnode-meta/wal/by-dev-rootcoord-dml_1/vchannel/"+noiseVChannel+"/schema/300"] = mustMarshal(t, &streamingpb.CollectionSchemaOfVChannel{
		Schema: &schemapb.CollectionSchema{
			Name:    "coll_42",
			Version: 5,
		},
		CheckpointTimeTick: 300,
	})

	return cli
}

func loadCollectionVersion(t *testing.T, cli kv.MetaKV, key string) int32 {
	t.Helper()
	v, err := cli.Load(context.Background(), key)
	require.NoError(t, err)
	info := &etcdpb.CollectionInfo{}
	require.NoError(t, proto.Unmarshal([]byte(v), info))
	return info.GetSchema().GetVersion()
}

func loadVChannelSchemaVersion(t *testing.T, cli kv.MetaKV, key string) int32 {
	t.Helper()
	v, err := cli.Load(context.Background(), key)
	require.NoError(t, err)
	schema := &streamingpb.CollectionSchemaOfVChannel{}
	require.NoError(t, proto.Unmarshal([]byte(v), schema))
	return schema.GetSchema().GetVersion()
}

func TestSchemaVersionCommand(t *testing.T) {
	basePath := testBasePath
	collectionKey := basePath + "/root-coord/collection/1"
	schemaKey := func(tick uint64) string {
		return basePath + "/streamingnode-meta/wal/" + testPChannel + "/vchannel/" + testVChannel + "/schema/" + strconv.FormatUint(tick, 10)
	}

	t.Run("dry run does not write", func(t *testing.T) {
		cli := seedSchemaVersionTestKV(t)
		cmd := NewComponent(cli, nil, basePath)

		err := cmd.SchemaVersionCommand(context.Background(), &SchemaVersionParam{
			CollectionID: testCollection,
			Version:      5,
		})
		require.NoError(t, err)

		require.Equal(t, int32(3), loadCollectionVersion(t, cli, collectionKey))
		require.Equal(t, int32(1), loadVChannelSchemaVersion(t, cli, schemaKey(100)))
		require.Equal(t, int32(2), loadVChannelSchemaVersion(t, cli, schemaKey(200)))
		require.Equal(t, int32(9), loadCollectionVersion(t, cli, basePath+"/root-coord/database/collection-info/7/2"))
	})

	t.Run("run repairs collection meta and every schema snapshot", func(t *testing.T) {
		cli := seedSchemaVersionTestKV(t)
		cmd := NewComponent(cli, nil, basePath)

		err := cmd.SchemaVersionCommand(context.Background(), &SchemaVersionParam{
			CollectionID: testCollection,
			Version:      5,
			Scope:        "all",
			ExecutionParam: framework.ExecutionParam{
				Run: true,
			},
		})
		require.NoError(t, err)

		require.Equal(t, int32(5), loadCollectionVersion(t, cli, collectionKey))
		require.Equal(t, int32(5), loadVChannelSchemaVersion(t, cli, schemaKey(100)))
		require.Equal(t, int32(5), loadVChannelSchemaVersion(t, cli, schemaKey(200)))

		// unrelated data must stay untouched.
		require.Equal(t, int32(9), loadCollectionVersion(t, cli, basePath+"/root-coord/database/collection-info/7/2"))
		require.Equal(t, int32(5), loadVChannelSchemaVersion(t, cli, basePath+"/streamingnode-meta/wal/by-dev-rootcoord-dml_1/vchannel/by-dev-rootcoord-dml_1_2v0/schema/300"))

		// schema keys keep their original checkpoint time tick suffix.
		require.Contains(t, cli.data, schemaKey(100))
		require.Contains(t, cli.data, schemaKey(200))
	})

	t.Run("run defaults to latest scope", func(t *testing.T) {
		cli := seedSchemaVersionTestKV(t)
		cmd := NewComponent(cli, nil, basePath)

		err := cmd.SchemaVersionCommand(context.Background(), &SchemaVersionParam{
			CollectionID: testCollection,
			Version:      6,
			ExecutionParam: framework.ExecutionParam{
				Run: true,
			},
		})
		require.NoError(t, err)

		// default scope is latest: only the newest non-dropped snapshot changes.
		require.Equal(t, int32(6), loadVChannelSchemaVersion(t, cli, schemaKey(200)))
		require.Equal(t, int32(1), loadVChannelSchemaVersion(t, cli, schemaKey(100)))
		require.Equal(t, int32(3), loadVChannelSchemaVersion(t, cli, schemaKey(300)))
	})

	t.Run("run with latest scope repairs only newest non-dropped snapshot", func(t *testing.T) {
		cli := seedSchemaVersionTestKV(t)
		cmd := NewComponent(cli, nil, basePath)

		err := cmd.SchemaVersionCommand(context.Background(), &SchemaVersionParam{
			CollectionID: testCollection,
			Version:      7,
			Scope:        "latest",
			ExecutionParam: framework.ExecutionParam{
				Run: true,
			},
		})
		require.NoError(t, err)

		// collection meta and newest (tick 200) snapshot repaired...
		require.Equal(t, int32(7), loadCollectionVersion(t, cli, collectionKey))
		require.Equal(t, int32(7), loadVChannelSchemaVersion(t, cli, schemaKey(200)))
		// ...older snapshots and the dropped one stay untouched.
		require.Equal(t, int32(1), loadVChannelSchemaVersion(t, cli, schemaKey(100)))
		require.Equal(t, int32(3), loadVChannelSchemaVersion(t, cli, schemaKey(300)))
	})

	t.Run("repairs collection meta stored in db path", func(t *testing.T) {
		cli := &fakeMetaKV{data: map[string]string{}}
		coll := &etcdpb.CollectionInfo{
			ID:   3,
			DbId: 7,
			Schema: &schemapb.CollectionSchema{
				Name:    "coll_3",
				Version: 2,
			},
		}
		key := basePath + "/root-coord/database/collection-info/7/3"
		cli.data[key] = mustMarshal(t, coll)
		cmd := NewComponent(cli, nil, basePath)

		err := cmd.SchemaVersionCommand(context.Background(), &SchemaVersionParam{
			CollectionID: 3,
			Version:      8,
			ExecutionParam: framework.ExecutionParam{
				Run: true,
			},
		})
		require.NoError(t, err)

		require.Equal(t, int32(8), loadCollectionVersion(t, cli, key))
	})

	t.Run("collection not found", func(t *testing.T) {
		cli := seedSchemaVersionTestKV(t)
		cmd := NewComponent(cli, nil, basePath)

		err := cmd.SchemaVersionCommand(context.Background(), &SchemaVersionParam{
			CollectionID: 999,
			Version:      5,
			ExecutionParam: framework.ExecutionParam{
				Run: true,
			},
		})
		require.Error(t, err)
	})

	t.Run("invalid collection id and version", func(t *testing.T) {
		cli := seedSchemaVersionTestKV(t)
		cmd := NewComponent(cli, nil, basePath)

		require.Error(t, cmd.SchemaVersionCommand(context.Background(), &SchemaVersionParam{CollectionID: 0, Version: 1}))
		require.Error(t, cmd.SchemaVersionCommand(context.Background(), &SchemaVersionParam{CollectionID: testCollection, Version: -1}))
		require.Error(t, cmd.SchemaVersionCommand(context.Background(), &SchemaVersionParam{CollectionID: testCollection, Version: 1 << 31}))
		require.Error(t, cmd.SchemaVersionCommand(context.Background(), &SchemaVersionParam{CollectionID: testCollection, Version: 1, Scope: "everything"}))
	})
}
