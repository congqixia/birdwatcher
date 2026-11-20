package repair

import (
	"context"
	"errors"
	"maps"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestParseSegmentState(t *testing.T) {
	for name, number := range commonpb.SegmentState_value {
		for _, input := range []string{name, " " + strings.ToLower(name) + " ", strconv.FormatInt(int64(number), 10)} {
			t.Run(input, func(t *testing.T) {
				state, err := parseSegmentState(input)
				require.NoError(t, err)
				require.Equal(t, commonpb.SegmentState(number), state)
			})
		}
	}
	for _, input := range []string{"", " ", "Drop", "unknown", "-1", "8", "2147483648", "4294967302", "6.0"} {
		t.Run("invalid/"+input, func(t *testing.T) {
			_, err := parseSegmentState(input)
			require.ErrorContains(t, err, "valid states:")
		})
	}
}

type segmentStateKV struct {
	*fakeMetaKV
	saveKeys []string
	failSave int
}

func (s *segmentStateKV) Save(ctx context.Context, key, value string) error {
	s.saveKeys = append(s.saveKeys, key)
	if len(s.saveKeys) == s.failSave {
		return errors.New("save failed")
	}
	return s.fakeMetaKV.Save(ctx, key, value)
}

func TestRepairSegmentStateCommand(t *testing.T) {
	const (
		basePath = "root/meta"
		segKey   = basePath + "/datacoord-meta/s/100/20/10"
	)
	for _, test := range []struct {
		name     string
		target   string
		run      bool
		failSave int
	}{
		{name: "dry run", target: "Dropped"},
		{name: "drop growing", target: "Dropped", run: true},
		{name: "numeric zero", target: "0", run: true},
		{name: "same state", target: "growing", run: true},
		{name: "backup failure", target: "Dropped", run: true, failSave: 1},
		{name: "update failure", target: "Dropped", run: true, failSave: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			segment := &datapb.SegmentInfo{
				ID: 10, CollectionID: 100, PartitionID: 20, State: commonpb.SegmentState_Growing,
				StorageVersion: 3, DataVersion: 7, DroppedAt: 123, NumOfRows: 50,
				ManifestPath: "manifest.avro", InsertChannel: "channel", IsImporting: true,
				Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 1}}}},
			}
			// Unknown protobuf fields must survive the state update as well.
			segment.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
			original := mustMarshal(t, segment)
			cli := &segmentStateKV{fakeMetaKV: &fakeMetaKV{data: map[string]string{
				segKey:                                   original,
				basePath + "/datacoord-meta/s/100/20/11": mustMarshal(t, &datapb.SegmentInfo{ID: 11, CollectionID: 100, PartitionID: 20}),
				basePath + "/datacoord-meta/binlog/100/20/10/100": "keep binlog",
			}}, failSave: test.failSave}
			before := maps.Clone(cli.data)
			component := NewComponent(cli, nil, basePath)
			param := &RepairSegmentStateParam{ExecutionParam: framework.ExecutionParam{Run: test.run}, SegmentID: 10, State: test.target}
			err := component.RepairSegmentStateCommand(context.Background(), param)
			if test.failSave != 0 {
				require.ErrorContains(t, err, "save failed")
				require.Len(t, cli.saveKeys, test.failSave)
				require.Equal(t, original, cli.data[segKey])
			} else {
				require.NoError(t, err)
				if test.run && test.target != "growing" {
					require.Len(t, cli.saveKeys, 2)
					require.Equal(t, segKey, cli.saveKeys[1])
					updated := &datapb.SegmentInfo{}
					require.NoError(t, proto.Unmarshal([]byte(cli.data[segKey]), updated))
					expected := proto.Clone(segment).(*datapb.SegmentInfo)
					expected.State, err = parseSegmentState(test.target)
					require.NoError(t, err)
					require.True(t, proto.Equal(expected, updated), "only State may change")
					require.NoError(t, component.RepairSegmentStateCommand(context.Background(), param))
					require.Len(t, cli.saveKeys, 2, "repeated repair must not write again")
				} else {
					require.Empty(t, cli.saveKeys)
					require.Equal(t, before, cli.data)
				}
			}
			if len(cli.saveKeys) == 2 {
				require.True(t, strings.HasPrefix(cli.saveKeys[0], "birdwatcher/backup/segment-state/"))
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

func TestRepairSegmentStateRejectsInvalidInput(t *testing.T) {
	const segKey = "root/datacoord-meta/s/100/20/10"
	for _, test := range []struct {
		name      string
		segmentID int64
		state     string
		data      map[string]string
		wantErr   string
	}{
		{name: "missing segment", state: "Dropped", wantErr: "must be positive"},
		{name: "negative segment", segmentID: -1, state: "Dropped", wantErr: "must be positive"},
		{name: "missing state", segmentID: 10, wantErr: "invalid segment state"},
		{name: "invalid state", segmentID: 10, state: "8", wantErr: "invalid segment state"},
		{name: "not found", segmentID: 10, state: "Dropped", wantErr: "not found"},
		{name: "duplicate segment", segmentID: 10, state: "Dropped", data: map[string]string{
			segKey:                            mustMarshal(t, &datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 20}),
			"root/datacoord-meta/s/100/21/10": mustMarshal(t, &datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 21}),
		}, wantErr: "found 2 segment records"},
		{name: "key mismatch", segmentID: 10, state: "Dropped", data: map[string]string{
			segKey: mustMarshal(t, &datapb.SegmentInfo{ID: 10, CollectionID: 999, PartitionID: 20}),
		}, wantErr: "does not match its key"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cli := &segmentStateKV{fakeMetaKV: &fakeMetaKV{data: test.data}}
			err := NewComponent(cli, nil, "root").RepairSegmentStateCommand(context.Background(), &RepairSegmentStateParam{
				ExecutionParam: framework.ExecutionParam{Run: true}, SegmentID: test.segmentID, State: test.state,
			})
			require.ErrorContains(t, err, test.wantErr)
			require.Empty(t, cli.saveKeys)
		})
	}
}

func TestRepairSegmentStateCommandRegistered(t *testing.T) {
	const segKey = "root/datacoord-meta/s/100/20/10"
	original := mustMarshal(t, &datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 20, State: commonpb.SegmentState_Growing})
	cli := &segmentStateKV{fakeMetaKV: &fakeMetaKV{data: map[string]string{segKey: original}}}
	host := &struct{ *framework.CmdState }{CmdState: framework.NewCmdState("host", nil)}
	root := &cobra.Command{SilenceUsage: true, SilenceErrors: true}
	host.MergeFunctionCommandsFrom(root, host, NewComponent(cli, nil, "root"))
	root.SetArgs([]string{"repair", "segment-state", "--segment", "10", "--state", "Dropped"})
	require.NoError(t, root.Execute())
	require.Empty(t, cli.saveKeys)
	root.SetArgs([]string{"repair", "segment-state", "--segment", "10", "--state", "Dropped", "--run"})
	require.NoError(t, root.Execute())
	updated := &datapb.SegmentInfo{}
	require.NoError(t, proto.Unmarshal([]byte(cli.data[segKey]), updated))
	require.Equal(t, commonpb.SegmentState_Dropped, updated.GetState())
}
