package healthz

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestStorageV3ManifestMissingCheck(t *testing.T) {
	item, ok := GetHealthzCheckItem("STORAGE_V3_MANIFEST_MISSING")
	require.True(t, ok)
	require.Equal(t, "STORAGE_V3_MANIFEST_MISSING", item.Name())
	require.NotEmpty(t, item.Description())

	const basePath = "root/meta"
	segments := []*datapb.SegmentInfo{
		{ID: 1, StorageVersion: 3, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1},
		{ID: 2, StorageVersion: 3, State: commonpb.SegmentState_Growing, Level: datapb.SegmentLevel_L1},
		{ID: 3, StorageVersion: 3, State: commonpb.SegmentState_Dropped, Level: datapb.SegmentLevel_L0},
		{ID: 4, StorageVersion: 3, ManifestPath: "files/insert_log/100/10/4/_metadata/manifest-1.avro"},
		{ID: 5, StorageVersion: 0},
		{ID: 6, StorageVersion: 1},
		{ID: 7, StorageVersion: 2},
		{ID: 8, StorageVersion: 4},
	}
	cli := &healthzCheckKV{data: make(map[string]string)}
	for _, segment := range segments {
		segment.CollectionID = 100
		segment.PartitionID = 10
		cli.data[segmentKey(basePath, 100, 10, segment.GetID())] = healthzProtoValue(t, segment)
	}
	// Metadata from other instances must not be included in the scan.
	cli.data[segmentKey("other/meta", 100, 10, 9)] = healthzProtoValue(t, &datapb.SegmentInfo{
		ID: 9, CollectionID: 100, PartitionID: 10, StorageVersion: 3,
	})

	reports, err := item.Check(context.Background(), cli, basePath)
	require.NoError(t, err)
	require.Len(t, reports, 3)
	for idx, report := range reports {
		segment := segments[idx]
		require.Equal(t, item.Name(), report.Item)
		require.Contains(t, report.Msg, "no manifest path")
		require.Equal(t, map[string]any{
			"segment_id":      segment.GetID(),
			"collection_id":   int64(100),
			"partition_id":    int64(10),
			"state":           segment.GetState().String(),
			"level":           segment.GetLevel().String(),
			"storage_version": int64(3),
			"manifest_path":   "",
		}, report.Extra)
	}

	t.Run("no matches", func(t *testing.T) {
		for _, segment := range segments[:3] {
			delete(cli.data, segmentKey(basePath, 100, 10, segment.GetID()))
		}
		reports, err := item.Check(context.Background(), cli, basePath)
		require.NoError(t, err)
		require.Empty(t, reports)
	})
}
