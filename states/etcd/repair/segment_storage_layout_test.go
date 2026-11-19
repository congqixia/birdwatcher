package repair

import (
	"context"
	"errors"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type segmentStorageLayoutKV struct {
	kv.MetaKV
	data           map[string]string
	multiSaveKeys  [][]string
	multiSaveValue [][]string
	multiSaveErr   error
}

func (s *segmentStorageLayoutKV) Load(ctx context.Context, key string, opts ...kv.LoadOption) (string, error) {
	value, ok := s.data[key]
	if !ok {
		return "", kv.ErrKeyNotFound
	}
	return value, nil
}

func (s *segmentStorageLayoutKV) LoadWithPrefix(ctx context.Context, prefix string, opts ...kv.LoadOption) ([]string, []string, error) {
	keys := make([]string, 0)
	for key := range s.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, s.data[key])
	}
	return keys, values, nil
}

func (s *segmentStorageLayoutKV) MultiSave(ctx context.Context, keys, values []string) error {
	s.multiSaveKeys = append(s.multiSaveKeys, append([]string(nil), keys...))
	s.multiSaveValue = append(s.multiSaveValue, append([]string(nil), values...))
	if s.multiSaveErr != nil {
		return s.multiSaveErr
	}
	for idx, key := range keys {
		s.data[key] = values[idx]
	}
	return nil
}

func TestParseColumnGroupMappings(t *testing.T) {
	mappings, err := parseColumnGroupMappings([]string{"0:100:1:0", "101:101"})
	require.NoError(t, err)
	require.Equal(t, map[int64][]int64{
		0:   {0, 1, 100},
		101: {101},
	}, mappings)
}

func TestParseColumnGroupMappingsRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		input []string
	}{
		{name: "empty", input: nil},
		{name: "missing children", input: []string{"100"}},
		{name: "duplicate group", input: []string{"100:100", "100:101"}},
		{name: "duplicate child in group", input: []string{"100:100:100"}},
		{name: "negative id", input: []string{"-1:100"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseColumnGroupMappings(test.input)
			require.Error(t, err)
		})
	}
}

func TestPrepareSegmentStorageLayout(t *testing.T) {
	segment := &datapb.SegmentInfo{
		ID:             10,
		StorageVersion: 1,
		DataVersion:    7,
	}
	records := []fieldBinlogRecord{
		{
			key: "binlog/0",
			value: &datapb.FieldBinlog{
				FieldID: 0,
			},
		},
		{
			key: "binlog/101",
			value: &datapb.FieldBinlog{
				FieldID:     101,
				ChildFields: []int64{999},
			},
		},
	}
	mappings := map[int64][]int64{
		0:   {0, 1, 100},
		101: {101},
	}
	validFields := map[int64]struct{}{
		0: {}, 1: {}, 100: {}, 101: {},
	}

	change, err := prepareSegmentStorageLayout(segment, records, storageVersionV2, mappings, validFields, false)
	require.NoError(t, err)
	require.True(t, change.changed)
	require.EqualValues(t, storageVersionV2, change.segment.GetStorageVersion())
	require.EqualValues(t, 8, change.segment.GetDataVersion())
	require.Equal(t, []int64{0, 1, 100}, change.fieldBinlogs[0].GetChildFields())
	require.Equal(t, []int64{101}, change.fieldBinlogs[1].GetChildFields())
	require.Equal(t, []string{"binlog/0", "binlog/101"}, change.fieldBinlogKeys)

	require.EqualValues(t, 1, segment.GetStorageVersion(), "input segment must not be mutated")
	require.Empty(t, records[0].value.GetChildFields(), "input FieldBinlog must not be mutated")
	require.Equal(t, []int64{999}, records[1].value.GetChildFields(), "input FieldBinlog must not be mutated")
}

func TestPrepareSegmentStorageLayoutNoChange(t *testing.T) {
	segment := &datapb.SegmentInfo{
		ID:             10,
		StorageVersion: storageVersionV2,
		DataVersion:    7,
	}
	records := []fieldBinlogRecord{{
		key: "binlog/100",
		value: &datapb.FieldBinlog{
			FieldID:     100,
			ChildFields: []int64{101, 100},
		},
	}}

	change, err := prepareSegmentStorageLayout(
		segment,
		records,
		storageVersionV2,
		map[int64][]int64{100: {100, 101}},
		map[int64]struct{}{100: {}, 101: {}},
		false,
	)
	require.NoError(t, err)
	require.False(t, change.changed)
	require.EqualValues(t, 7, change.segment.GetDataVersion())
	require.Equal(t, []int64{100, 101}, change.fieldBinlogs[0].GetChildFields())
}

func TestPrepareSegmentStorageLayoutRequiresCompleteMapping(t *testing.T) {
	records := []fieldBinlogRecord{
		{key: "binlog/100", value: &datapb.FieldBinlog{FieldID: 100}},
		{key: "binlog/101", value: &datapb.FieldBinlog{FieldID: 101}},
	}

	_, err := prepareSegmentStorageLayout(
		&datapb.SegmentInfo{},
		records,
		storageVersionV2,
		map[int64][]int64{100: {100}, 102: {102}},
		map[int64]struct{}{100: {}, 102: {}},
		false,
	)
	require.ErrorContains(t, err, "missing=[101]")
	require.ErrorContains(t, err, "unknown=[102]")
}

func TestPrepareSegmentStorageLayoutRejectsDuplicateAndUnknownChildren(t *testing.T) {
	records := []fieldBinlogRecord{
		{key: "binlog/100", value: &datapb.FieldBinlog{FieldID: 100}},
		{key: "binlog/101", value: &datapb.FieldBinlog{FieldID: 101}},
	}

	_, err := prepareSegmentStorageLayout(
		&datapb.SegmentInfo{},
		records,
		storageVersionV2,
		map[int64][]int64{100: {100}, 101: {100}},
		map[int64]struct{}{100: {}},
		false,
	)
	require.ErrorContains(t, err, "belongs to both column group")

	_, err = prepareSegmentStorageLayout(
		&datapb.SegmentInfo{},
		records,
		storageVersionV2,
		map[int64][]int64{100: {100}, 101: {999}},
		map[int64]struct{}{100: {}},
		false,
	)
	require.ErrorContains(t, err, "not present in the current collection schema")

	_, err = prepareSegmentStorageLayout(
		&datapb.SegmentInfo{},
		records,
		storageVersionV2,
		map[int64][]int64{100: {100}, 101: {999}},
		nil,
		true,
	)
	require.NoError(t, err)
}

func TestRepairSegmentStorageLayoutCommandBacksUpAndUpdatesAtomically(t *testing.T) {
	const (
		basePath    = "root"
		collection  = int64(100)
		partition   = int64(20)
		segmentID   = int64(10)
		dataVersion = int32(7)
	)
	segmentKey := path.Join(basePath, common.DCPrefix, common.SegmentMetaPrefix, "100", "20", "10")
	field0Key := path.Join(basePath, common.DCPrefix, "binlog", "100", "20", "10", "0")
	field101Key := path.Join(basePath, common.DCPrefix, "binlog", "100", "20", "10", "101")

	segment := &datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collection,
		PartitionID:    partition,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: 1,
		DataVersion:    dataVersion,
	}
	field0 := &datapb.FieldBinlog{FieldID: 0}
	field101 := &datapb.FieldBinlog{FieldID: 101, ChildFields: []int64{999}}

	mustMarshal := func(message proto.Message) string {
		value, err := proto.Marshal(message)
		require.NoError(t, err)
		return string(value)
	}
	originalSegment := mustMarshal(segment)
	originalField0 := mustMarshal(field0)
	originalField101 := mustMarshal(field101)
	cli := &segmentStorageLayoutKV{data: map[string]string{
		segmentKey:  originalSegment,
		field0Key:   originalField0,
		field101Key: originalField101,
	}}
	component := NewComponent(cli, nil, basePath)

	err := component.RepairSegmentStorageLayoutCommand(context.Background(), &RepairSegmentStorageLayoutParam{
		ExecutionParam:     framework.ExecutionParam{Run: true},
		SegmentID:          segmentID,
		StorageVersion:     storageVersionV2,
		ColumnGroups:       []string{"0:0:1:100", "101:101"},
		AllowUnknownFields: true,
	})
	require.NoError(t, err)
	require.Len(t, cli.multiSaveKeys, 2)
	require.Len(t, cli.multiSaveValue, 2)

	backupPrefix := path.Join("birdwatcher/backup/segment-storage-layout") + "/"
	require.Len(t, cli.multiSaveKeys[0], 3)
	for _, key := range cli.multiSaveKeys[0] {
		require.True(t, strings.HasPrefix(key, backupPrefix), key)
	}
	require.ElementsMatch(t, []string{originalSegment, originalField0, originalField101}, cli.multiSaveValue[0])

	require.Equal(t, []string{segmentKey, field0Key, field101Key}, cli.multiSaveKeys[1])
	updatedSegment := &datapb.SegmentInfo{}
	require.NoError(t, proto.Unmarshal([]byte(cli.data[segmentKey]), updatedSegment))
	require.EqualValues(t, storageVersionV2, updatedSegment.GetStorageVersion())
	require.EqualValues(t, dataVersion+1, updatedSegment.GetDataVersion())

	updatedField0 := &datapb.FieldBinlog{}
	require.NoError(t, proto.Unmarshal([]byte(cli.data[field0Key]), updatedField0))
	require.Equal(t, []int64{0, 1, 100}, updatedField0.GetChildFields())
	updatedField101 := &datapb.FieldBinlog{}
	require.NoError(t, proto.Unmarshal([]byte(cli.data[field101Key]), updatedField101))
	require.Equal(t, []int64{101}, updatedField101.GetChildFields())
}

func TestRepairSegmentStorageLayoutV0(t *testing.T) {
	const (
		basePath = "root/meta"
		segKey   = basePath + "/datacoord-meta/s/100/20/10"
		fieldKey = basePath + "/datacoord-meta/binlog/100/20/10/100"
	)
	for _, test := range []struct {
		name          string
		run           bool
		alreadyV0     bool
		embeddedOnly  bool
		backupFailure bool
		growing       bool
		noBinlogs     bool
	}{
		{name: "dry run"},
		{name: "run", run: true},
		{name: "already v0", run: true, alreadyV0: true},
		{name: "embedded binlogs only", run: true, embeddedOnly: true},
		{name: "backup failure", run: true, backupFailure: true},
		{name: "growing dry run", growing: true},
		{name: "growing run", run: true, growing: true},
		{name: "growing without binlogs", run: true, growing: true, noBinlogs: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			field := &datapb.FieldBinlog{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 123, EntriesNum: 10}}}
			segment := &datapb.SegmentInfo{
				ID: 10, CollectionID: 100, PartitionID: 20,
				State: commonpb.SegmentState_Flushed, StorageVersion: 3, DataVersion: 7,
				NumOfRows: 10, InsertChannel: "channel", Binlogs: []*datapb.FieldBinlog{field},
			}
			if test.alreadyV0 {
				segment.StorageVersion = 0
			}
			if test.growing {
				segment.State = commonpb.SegmentState_Growing
			}
			if test.noBinlogs {
				segment.Binlogs = nil
				segment.NumOfRows = 0
			}
			originalSegment := mustMarshal(t, segment)
			originalField := mustMarshal(t, field)
			cli := &segmentStorageLayoutKV{data: map[string]string{
				segKey: originalSegment, "unrelated": "keep",
			}}
			if !test.embeddedOnly && !test.noBinlogs {
				cli.data[fieldKey] = originalField
			}
			if test.backupFailure {
				cli.multiSaveErr = errors.New("backup unavailable")
			}
			component := NewComponent(cli, nil, basePath)
			param := &RepairSegmentStorageLayoutParam{
				ExecutionParam: framework.ExecutionParam{Run: test.run}, SegmentID: 10, StorageVersion: 0,
			}
			err := component.RepairSegmentStorageLayoutCommand(context.Background(), param)
			if test.backupFailure {
				require.ErrorIs(t, err, cli.multiSaveErr)
				require.Len(t, cli.multiSaveKeys, 1, "failed backup must prevent the update")
			} else {
				require.NoError(t, err)
			}
			if !test.run || test.alreadyV0 || test.backupFailure {
				require.Equal(t, originalSegment, cli.data[segKey])
				if !test.backupFailure {
					require.Empty(t, cli.multiSaveKeys)
				}
			} else {
				require.Len(t, cli.multiSaveKeys, 2)
				require.Len(t, cli.multiSaveKeys[0], 1)
				backupKey := cli.multiSaveKeys[0][0]
				require.True(t, strings.HasPrefix(backupKey, "birdwatcher/backup/segment-storage-layout/"))
				require.Equal(t, originalSegment, cli.data[backupKey])
				require.Equal(t, []string{segKey}, cli.multiSaveKeys[1], "only the segment metadata should be updated")
				updated := &datapb.SegmentInfo{}
				require.NoError(t, proto.Unmarshal([]byte(cli.data[segKey]), updated))
				expected := proto.Clone(segment).(*datapb.SegmentInfo)
				expected.StorageVersion = 0
				require.Equal(t, segment.GetDataVersion(), updated.GetDataVersion())
				require.True(t, proto.Equal(expected, updated), "all other segment fields must be preserved")
				// Repeating the repair must not write another backup or bump DataVersion.
				require.NoError(t, component.RepairSegmentStorageLayoutCommand(context.Background(), param))
				require.Len(t, cli.multiSaveKeys, 2)
			}
			if !test.embeddedOnly && !test.noBinlogs {
				require.Equal(t, originalField, cli.data[fieldKey])
			}
			require.Equal(t, "keep", cli.data["unrelated"])
		})
	}
}

func TestRepairSegmentStorageLayoutV0RejectsIncompatibleMetadata(t *testing.T) {
	for _, test := range []struct {
		name    string
		segment *datapb.SegmentInfo
		field   *datapb.FieldBinlog
		groups  []string
		wantErr string
	}{
		{name: "manifest", segment: &datapb.SegmentInfo{State: commonpb.SegmentState_Flushed, ManifestPath: "manifest.avro"}, wantErr: "has manifest path"},
		{name: "dropped", segment: &datapb.SegmentInfo{State: commonpb.SegmentState_Dropped}, wantErr: "only Growing or Flushed"},
		{name: "sealed", segment: &datapb.SegmentInfo{State: commonpb.SegmentState_Sealed}, wantErr: "only Growing or Flushed"},
		{name: "growing with manifest", segment: &datapb.SegmentInfo{State: commonpb.SegmentState_Growing, ManifestPath: "manifest.avro"}, wantErr: "has manifest path"},
		{name: "growing with column group", segment: &datapb.SegmentInfo{State: commonpb.SegmentState_Growing}, field: &datapb.FieldBinlog{FieldID: 100, ChildFields: []int64{100}}, wantErr: "has child fields"},
		{name: "standalone column group", field: &datapb.FieldBinlog{FieldID: 100, ChildFields: []int64{100, 101}}, wantErr: "has child fields"},
		{name: "embedded column group", segment: &datapb.SegmentInfo{State: commonpb.SegmentState_Flushed, Binlogs: []*datapb.FieldBinlog{{FieldID: 100, ChildFields: []int64{100}}}}, wantErr: "embedded FieldBinlog"},
		{name: "column group parameter", groups: []string{"100:100"}, wantErr: "only supported for storage version 2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			segment := test.segment
			if segment == nil {
				segment = &datapb.SegmentInfo{State: commonpb.SegmentState_Flushed}
			}
			segment.ID, segment.CollectionID, segment.PartitionID, segment.StorageVersion = 10, 100, 20, 3
			const segKey = "root/datacoord-meta/s/100/20/10"
			original := mustMarshal(t, segment)
			cli := &segmentStorageLayoutKV{data: map[string]string{segKey: original}}
			if test.field != nil {
				cli.data["root/datacoord-meta/binlog/100/20/10/100"] = mustMarshal(t, test.field)
			}
			err := NewComponent(cli, nil, "root").RepairSegmentStorageLayoutCommand(context.Background(), &RepairSegmentStorageLayoutParam{
				ExecutionParam: framework.ExecutionParam{Run: true}, SegmentID: 10, StorageVersion: 0, ColumnGroups: test.groups,
			})
			require.ErrorContains(t, err, test.wantErr)
			require.Empty(t, cli.multiSaveKeys)
			require.Equal(t, original, cli.data[segKey])
		})
	}
}

func TestRepairSegmentStorageLayoutV0CommandFlags(t *testing.T) {
	const segKey = "root/datacoord-meta/s/100/20/10"
	original := mustMarshal(t, &datapb.SegmentInfo{
		ID: 10, CollectionID: 100, PartitionID: 20, State: commonpb.SegmentState_Flushed,
		StorageVersion: 3, Level: datapb.SegmentLevel_L0,
	})
	cli := &segmentStorageLayoutKV{data: map[string]string{segKey: original}}
	host := &struct{ *framework.CmdState }{CmdState: framework.NewCmdState("host", nil)}
	root := &cobra.Command{SilenceUsage: true, SilenceErrors: true}
	host.MergeFunctionCommandsFrom(root, host, NewComponent(cli, nil, "root"))

	root.SetArgs([]string{"repair", "segment-storage-layout", "--segment", "10", "--run"})
	require.NoError(t, root.Execute())
	require.Empty(t, cli.multiSaveKeys, "omitting --storageVersion must not default to version 0")

	root.SetArgs([]string{"repair", "segment-storage-layout", "--segment", "10", "--storageVersion", "0", "--run"})
	require.NoError(t, root.Execute())
	require.Len(t, cli.multiSaveKeys, 2)
	updated := &datapb.SegmentInfo{}
	require.NoError(t, proto.Unmarshal([]byte(cli.data[segKey]), updated))
	require.Zero(t, updated.GetStorageVersion())
}

func TestRepairSegmentStorageLayoutV2RejectsGrowing(t *testing.T) {
	const segKey = "root/datacoord-meta/s/100/20/10"
	original := mustMarshal(t, &datapb.SegmentInfo{
		ID: 10, CollectionID: 100, PartitionID: 20, State: commonpb.SegmentState_Growing,
		StorageVersion: 3,
	})
	cli := &segmentStorageLayoutKV{data: map[string]string{segKey: original}}
	err := NewComponent(cli, nil, "root").RepairSegmentStorageLayoutCommand(context.Background(), &RepairSegmentStorageLayoutParam{
		ExecutionParam: framework.ExecutionParam{Run: true}, SegmentID: 10,
		StorageVersion: storageVersionV2, ColumnGroups: []string{"100:100"},
	})
	require.ErrorContains(t, err, "only Flushed")
	require.Empty(t, cli.multiSaveKeys)
	require.Equal(t, original, cli.data[segKey])
}
