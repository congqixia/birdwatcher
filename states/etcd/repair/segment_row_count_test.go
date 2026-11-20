package repair

import (
	"context"
	"maps"
	"math"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestRepairSegmentRowCountCommand(t *testing.T) {
	const (
		basePath = "root/meta"
		segKey   = basePath + "/datacoord-meta/s/100/20/10"
	)
	for _, test := range []struct {
		name     string
		rowCount int64
		run      bool
		failSave int
	}{
		{name: "dry run", rowCount: 100},
		{name: "update growing", rowCount: 100, run: true},
		{name: "zero rows", rowCount: 0, run: true},
		{name: "max int64", rowCount: math.MaxInt64, run: true},
		{name: "same count", rowCount: 50, run: true},
		{name: "backup failure", rowCount: 100, run: true, failSave: 1},
		{name: "update failure", rowCount: 100, run: true, failSave: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			segment := &datapb.SegmentInfo{
				ID: 10, CollectionID: 100, PartitionID: 20, State: commonpb.SegmentState_Growing,
				StorageVersion: 3, DataVersion: 7, DroppedAt: 123, NumOfRows: 50,
				ManifestPath: "manifest.avro", InsertChannel: "channel", IsImporting: true,
				Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 1, EntriesNum: 50}}}},
			}
			segment.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
			original := mustMarshal(t, segment)
			cli := &segmentStateKV{fakeMetaKV: &fakeMetaKV{data: map[string]string{
				segKey:                                   original,
				basePath + "/datacoord-meta/s/100/20/11": mustMarshal(t, &datapb.SegmentInfo{ID: 11, CollectionID: 100, PartitionID: 20, NumOfRows: 200}),
				basePath + "/datacoord-meta/binlog/100/20/10/100": "keep binlog",
			}}, failSave: test.failSave}
			before := maps.Clone(cli.data)
			component := NewComponent(cli, nil, basePath)
			param := &RepairSegmentRowCountParam{ExecutionParam: framework.ExecutionParam{Run: test.run}, SegmentID: 10, RowCount: test.rowCount}
			err := component.RepairSegmentRowCountCommand(context.Background(), param)
			switch {
			case test.failSave != 0:
				require.ErrorContains(t, err, "save failed")
				require.Len(t, cli.saveKeys, test.failSave)
				require.Equal(t, original, cli.data[segKey])
			case !test.run || test.rowCount == segment.GetNumOfRows():
				require.NoError(t, err)
				require.Empty(t, cli.saveKeys)
				require.Equal(t, before, cli.data)
			default:
				require.NoError(t, err)
				require.Len(t, cli.saveKeys, 2)
				require.Equal(t, segKey, cli.saveKeys[1])
				updated := &datapb.SegmentInfo{}
				require.NoError(t, proto.Unmarshal([]byte(cli.data[segKey]), updated))
				expected := proto.Clone(segment).(*datapb.SegmentInfo)
				expected.NumOfRows = test.rowCount
				require.True(t, proto.Equal(expected, updated), "only NumOfRows may change")
				require.NoError(t, component.RepairSegmentRowCountCommand(context.Background(), param))
				require.Len(t, cli.saveKeys, 2, "repeated repair must not write again")
			}
			if len(cli.saveKeys) == 2 {
				require.True(t, strings.HasPrefix(cli.saveKeys[0], "birdwatcher/backup/segment-row-count/"))
				require.Equal(t, original, cli.data[cli.saveKeys[0]])
			}
			for key, value := range before {
				if key != segKey {
					require.Equal(t, value, cli.data[key])
				}
			}
		})
	}
}

func TestRepairSegmentRowCountRejectsInvalidInput(t *testing.T) {
	const segKey = "root/datacoord-meta/s/100/20/10"
	for _, test := range []struct {
		name      string
		segmentID int64
		rowCount  int64
		data      map[string]string
		wantErr   string
	}{
		{name: "missing segment", rowCount: 100, wantErr: "must be positive"},
		{name: "negative segment", segmentID: -1, rowCount: 100, wantErr: "must be positive"},
		{name: "missing row count", segmentID: 10, rowCount: -1, wantErr: "must be specified and non-negative"},
		{name: "negative row count", segmentID: 10, rowCount: -100, wantErr: "must be specified and non-negative"},
		{name: "not found", segmentID: 10, rowCount: 100, wantErr: "not found"},
		{name: "duplicate segment", segmentID: 10, rowCount: 100, data: map[string]string{
			segKey:                            mustMarshal(t, &datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 20}),
			"root/datacoord-meta/s/100/21/10": mustMarshal(t, &datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 21}),
		}, wantErr: "found 2 segment records"},
		{name: "key mismatch", segmentID: 10, rowCount: 100, data: map[string]string{
			segKey: mustMarshal(t, &datapb.SegmentInfo{ID: 10, CollectionID: 999, PartitionID: 20}),
		}, wantErr: "does not match its key"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cli := &segmentStateKV{fakeMetaKV: &fakeMetaKV{data: test.data}}
			err := NewComponent(cli, nil, "root").RepairSegmentRowCountCommand(context.Background(), &RepairSegmentRowCountParam{
				ExecutionParam: framework.ExecutionParam{Run: true}, SegmentID: test.segmentID, RowCount: test.rowCount,
			})
			require.ErrorContains(t, err, test.wantErr)
			require.Empty(t, cli.saveKeys)
		})
	}
}

func TestRepairSegmentRowCountCommandFlags(t *testing.T) {
	const segKey = "root/datacoord-meta/s/100/20/10"
	for _, test := range []struct {
		name    string
		args    []string
		writes  int
		wantErr bool
	}{
		{name: "missing row count", args: []string{"--run"}},
		{name: "dry run", args: []string{"--rowCount", "0"}},
		{name: "explicit zero", args: []string{"--rowCount", "0", "--run"}, writes: 2},
		{name: "overflow", args: []string{"--rowCount", "9223372036854775808", "--run"}, wantErr: true},
		{name: "not an integer", args: []string{"--rowCount", "1.5", "--run"}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := mustMarshal(t, &datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 20, State: commonpb.SegmentState_Flushed, NumOfRows: 50})
			cli := &segmentStateKV{fakeMetaKV: &fakeMetaKV{data: map[string]string{segKey: original}}}
			host := &struct{ *framework.CmdState }{CmdState: framework.NewCmdState("host", nil)}
			root := &cobra.Command{SilenceUsage: true, SilenceErrors: true}
			host.MergeFunctionCommandsFrom(root, host, NewComponent(cli, nil, "root"))
			root.SetArgs(append([]string{"repair", "segment-row-count", "--segment", "10"}, test.args...))
			err := root.Execute()
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, cli.saveKeys, test.writes)
			if test.writes == 0 {
				require.Equal(t, original, cli.data[segKey])
			} else {
				updated := &datapb.SegmentInfo{}
				require.NoError(t, proto.Unmarshal([]byte(cli.data[segKey]), updated))
				require.Zero(t, updated.GetNumOfRows())
			}
		})
	}
}
