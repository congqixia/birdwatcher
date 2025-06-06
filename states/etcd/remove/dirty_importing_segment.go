package remove

import (
	"context"
	"fmt"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type DirtyImportingSegment struct {
	framework.ParamBase `use:"remove dirty-importing-segment" desc:"remove dirty importing segments with 0 rows"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to filter with"`
	Ts                  int64 `name:"ts" default:"0" desc:"only remove segments with ts less than this value"`
	Run                 bool  `name:"run" default:"false" desc:"flag to control actually run or dry"`
}

// DirtyImportingSegmentCommand returns command to remove
func (c *ComponentRemove) DirtyImportingSegmentCommand(ctx context.Context, p *DirtyImportingSegment) error {
	fmt.Println("start to remove dirty importing segment")
	segments, err := common.ListSegments(ctx, c.client, c.basePath, func(segment *models.Segment) bool {
		return (p.CollectionID == 0 || segment.CollectionID == p.CollectionID)
	})
	if err != nil {
		return err
	}

	groups := lo.GroupBy(segments, func(segment *models.Segment) int64 {
		return segment.CollectionID
	})

	cnt := 0
	for collectionID, segments := range groups {
		for _, segment := range segments {
			if segment.State == commonpb.SegmentState_Importing {
				segmentTs := segment.GetDmlPosition().GetTimestamp()
				if segmentTs == 0 {
					segmentTs = segment.GetStartPosition().GetTimestamp()
				}
				if segment.NumOfRows == 0 && segmentTs < uint64(p.Ts) {
					cnt++
					if p.Run {
						err := common.RemoveSegmentByID(ctx, c.client, c.basePath, segment.CollectionID, segment.PartitionID, segment.ID)
						if err != nil {
							fmt.Printf("failed to remove segment %d, err: %s\n", segment.ID, err.Error())
						}
						fmt.Printf("collection %d, segment %d is dirty importing with 0 rows, remove done\n", collectionID, segment.ID)
					} else {
						fmt.Printf("collection %d, segment %d is dirty importing with 0 rows\n", collectionID, segment.ID)
					}
				} else {
					fmt.Printf("collection %d, segment %d is dirty importing with %d rows, ts=%d, skip it\n", collectionID, segment.ID, segment.NumOfRows, segmentTs)
				}
			}
		}
	}

	if p.Run {
		fmt.Printf("finish to remove '%d' dirty importing segments\n", cnt)
	} else {
		fmt.Printf("found '%d' dirty importing segments\n", cnt)
	}
	return nil
}
