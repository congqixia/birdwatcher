package repair

import (
	"context"
	"strconv"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// seedLoadFieldsTestKV populates a fake MetaKV with one loaded collection
// whose persisted load info carries a stale LoadFields snapshot (missing the
// fields that were added after the load), plus unrelated noise data (another
// collection and another load info) that must stay untouched.
func seedLoadFieldsTestKV(t *testing.T) *fakeMetaKV {
	t.Helper()
	cli := &fakeMetaKV{data: map[string]string{}}

	// target collection meta (legacy path).
	coll := &etcdpb.CollectionInfo{
		ID: testCollection,
		Schema: &schemapb.CollectionSchema{
			Name: "coll_1",
		},
	}
	cli.data[testBasePath+"/root-coord/collection/1"] = mustMarshal(t, coll)

	// field schemas stored separately; 102 is the field added after load.
	// fieldID 1 mimics a system column that must never be loaded.
	for _, field := range []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", IsPrimaryKey: true},
		{FieldID: 1, Name: "Timestamp"},
		{FieldID: 101, Name: "vec"},
		{FieldID: 102, Name: "new_value", Nullable: true},
		// fieldID 103 is configured with field.skipLoad=true and must be
		// excluded from the backfilled LoadFields.
		{FieldID: 103, Name: "lazy_value", TypeParams: []*commonpb.KeyValuePair{
			{Key: "field.skipLoad", Value: "true"},
		}},
		// fieldID 104 has an unparseable skipLoad config; mirroring
		// GetCollectionLoadFields, the config is ignored and the field loads.
		{FieldID: 104, Name: "broken_value", TypeParams: []*commonpb.KeyValuePair{
			{Key: "field.skipLoad", Value: "not-a-bool"},
		}},
	} {
		cli.data[testBasePath+"/root-coord/fields/1/"+strconv.FormatInt(field.FieldID, 10)] = mustMarshal(t, field)
	}

	// a struct-array field whose sub-fields also carry loadable ids.
	structField := &schemapb.StructArrayFieldSchema{
		FieldID: 200,
		Name:    "struct_arr",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 201, Name: "sub_int"},
			{FieldID: 202, Name: "sub_str"},
		},
	}
	cli.data[testBasePath+"/root-coord/struct-array-fields/1/200"] = mustMarshal(t, structField)

	// target collection load info with a stale LoadFields snapshot.
	cli.data[testBasePath+"/querycoord-collection-loadinfo/1"] = mustMarshal(t, &querypb.CollectionLoadInfo{
		CollectionID: testCollection,
		Status:       querypb.LoadStatus_Loading,
		LoadFields:   []int64{100},
	})

	// unrelated collection (db path) with its own load info, must stay untouched.
	noise := &etcdpb.CollectionInfo{
		ID:   2,
		DbId: 7,
		Schema: &schemapb.CollectionSchema{
			Name: "coll_2",
		},
	}
	cli.data[testBasePath+"/root-coord/database/collection-info/7/2"] = mustMarshal(t, noise)
	cli.data[testBasePath+"/root-coord/fields/2/100"] = mustMarshal(t, &schemapb.FieldSchema{FieldID: 100, Name: "pk2"})
	cli.data[testBasePath+"/querycoord-collection-loadinfo/2"] = mustMarshal(t, &querypb.CollectionLoadInfo{
		CollectionID: 2,
		LoadFields:   []int64{100},
	})

	return cli
}

func loadCollectionLoadFields(t *testing.T, cli *fakeMetaKV, collectionID int64) []int64 {
	t.Helper()
	v, err := cli.Load(context.Background(), testBasePath+"/querycoord-collection-loadinfo/"+strconv.FormatInt(collectionID, 10))
	require.NoError(t, err)
	info := &querypb.CollectionLoadInfo{}
	require.NoError(t, proto.Unmarshal([]byte(v), info))
	return info.GetLoadFields()
}

func TestLoadFieldsCommand(t *testing.T) {
	basePath := testBasePath
	// sorted; system column 1 excluded, skipLoad field 103 excluded, and
	// field 104 kept because its unparseable skipLoad config is ignored.
	expected := []int64{100, 101, 102, 104, 201, 202}

	t.Run("dry run does not write", func(t *testing.T) {
		cli := seedLoadFieldsTestKV(t)
		cmd := NewComponent(cli, nil, basePath)

		err := cmd.LoadFieldsCommand(context.Background(), &LoadFieldsParam{
			CollectionID: testCollection,
		})
		require.NoError(t, err)

		require.Equal(t, []int64{100}, loadCollectionLoadFields(t, cli, testCollection))
		require.Equal(t, []int64{100}, loadCollectionLoadFields(t, cli, 2))
	})

	t.Run("run backfills load fields with the full schema", func(t *testing.T) {
		cli := seedLoadFieldsTestKV(t)
		cmd := NewComponent(cli, nil, basePath)

		err := cmd.LoadFieldsCommand(context.Background(), &LoadFieldsParam{
			CollectionID: testCollection,
			ExecutionParam: framework.ExecutionParam{
				Run: true,
			},
		})
		require.NoError(t, err)

		require.Equal(t, expected, loadCollectionLoadFields(t, cli, testCollection))

		// unrelated load info must stay untouched.
		require.Equal(t, []int64{100}, loadCollectionLoadFields(t, cli, 2))
	})

	t.Run("run repairs every matched load info record", func(t *testing.T) {
		cli := seedLoadFieldsTestKV(t)
		// a second load info record for the same collection (e.g. a db-scoped
		// key) must also be repaired.
		cli.data[testBasePath+"/querycoord-collection-loadinfo/1/extra"] = mustMarshal(t, &querypb.CollectionLoadInfo{
			CollectionID: testCollection,
			LoadFields:   []int64{101},
		})
		cmd := NewComponent(cli, nil, basePath)

		err := cmd.LoadFieldsCommand(context.Background(), &LoadFieldsParam{
			CollectionID: testCollection,
			ExecutionParam: framework.ExecutionParam{
				Run: true,
			},
		})
		require.NoError(t, err)

		require.Equal(t, expected, loadCollectionLoadFields(t, cli, testCollection))
		v, err := cli.Load(context.Background(), testBasePath+"/querycoord-collection-loadinfo/1/extra")
		require.NoError(t, err)
		extra := &querypb.CollectionLoadInfo{}
		require.NoError(t, proto.Unmarshal([]byte(v), extra))
		require.Equal(t, expected, extra.GetLoadFields())
	})

	t.Run("collection not found", func(t *testing.T) {
		cli := seedLoadFieldsTestKV(t)
		cmd := NewComponent(cli, nil, basePath)

		err := cmd.LoadFieldsCommand(context.Background(), &LoadFieldsParam{
			CollectionID: 999,
			ExecutionParam: framework.ExecutionParam{
				Run: true,
			},
		})
		require.Error(t, err)
	})

	t.Run("collection has no load info", func(t *testing.T) {
		cli := seedLoadFieldsTestKV(t)
		delete(cli.data, testBasePath+"/querycoord-collection-loadinfo/1")
		cmd := NewComponent(cli, nil, basePath)

		err := cmd.LoadFieldsCommand(context.Background(), &LoadFieldsParam{
			CollectionID: testCollection,
			ExecutionParam: framework.ExecutionParam{
				Run: true,
			},
		})
		require.Error(t, err)
	})

	t.Run("invalid collection id", func(t *testing.T) {
		cli := seedLoadFieldsTestKV(t)
		cmd := NewComponent(cli, nil, basePath)

		err := cmd.LoadFieldsCommand(context.Background(), &LoadFieldsParam{CollectionID: 0})
		require.Error(t, err)
	})
}

// TestLoadFieldsCommandRegistered verifies the command is discoverable through
// the framework's reflection-based registration under `repair load-fields`.
func TestLoadFieldsCommandRegistered(t *testing.T) {
	cli := &fakeMetaKV{data: map[string]string{}}
	component := NewComponent(cli, nil, testBasePath)
	host := &struct{ *framework.CmdState }{CmdState: framework.NewCmdState("host", nil)}
	root := &cobra.Command{SilenceUsage: true, SilenceErrors: true}
	host.MergeFunctionCommandsFrom(root, host, component)

	cmd, _, err := root.Find([]string{"repair", "load-fields"})
	require.NoError(t, err)
	require.NotNil(t, cmd)
	require.Equal(t, "load-fields", cmd.Use)

	// the collectionID flag must be exposed.
	flag := cmd.Flags().Lookup("collectionID")
	require.NotNil(t, flag)
	flagRun := cmd.Flags().Lookup("run")
	require.NotNil(t, flagRun)
}
