package healthz

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/states/etcd/common"
	metakv "github.com/milvus-io/birdwatcher/states/kv"
)

type StorageV3ManifestMissing struct {
	checkItemBase
}

func newStorageV3ManifestMissing() *StorageV3ManifestMissing {
	return &StorageV3ManifestMissing{
		checkItemBase: checkItemBase{
			name:        "STORAGE_V3_MANIFEST_MISSING",
			description: `Checks whether segments with storage version 3 have an empty manifest path, regardless of state or level.`,
		},
	}
}

func (i *StorageV3ManifestMissing) Check(ctx context.Context, client metakv.MetaKV, basePath string) ([]*HealthzCheckReport, error) {
	segments, err := common.ListSegments(ctx, client, basePath)
	if err != nil {
		return nil, err
	}

	var results []*HealthzCheckReport
	for _, segment := range segments {
		if segment.GetStorageVersion() != 3 || segment.GetManifestPath() != "" {
			continue
		}

		results = append(results, &HealthzCheckReport{
			Item: i.Name(),
			Msg:  fmt.Sprintf("Segment %d has storage version 3 but no manifest path", segment.GetID()),
			Extra: map[string]any{
				"segment_id":      segment.GetID(),
				"collection_id":   segment.GetCollectionID(),
				"partition_id":    segment.GetPartitionID(),
				"state":           segment.GetState().String(),
				"level":           segment.GetLevel().String(),
				"storage_version": segment.GetStorageVersion(),
				"manifest_path":   segment.GetManifestPath(),
			},
		})
	}

	return results, nil
}
