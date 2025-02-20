package show

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
)

type SegmentNewParam struct {
	framework.ParamBase `use:"show segment-new" desc:"display segment information from data coord meta store" alias:"segments"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	PartitionID         int64  `name:"partition" default:"0" desc:"partition id to filter with"`
	SegmentID           int64  `name:"segment" default:"0" desc:"segment id to display"`
	Format              string `name:"format" default:"line" desc:"segment display format"`
	Detail              bool   `name:"detail" default:"false" desc:"flags indicating whether printing detail binlog info"`
	State               string `name:"state" default:"" desc:"target segment state"`
	Level               string `name:"level" default:"" desc:"target segment level"`
}

// SegmentCommand returns show segments command.
func (c *ComponentShow) SegmentNewCommand(ctx context.Context, p *SegmentNewParam) error {
	prefix := path.Join(c.metaPath, common.SegmentMetaPrefix) + "/"
	segments, _, err := common.ListProtoObjectsV2(ctx, c.client, prefix, func(segment *datapb.SegmentInfo) bool {
		return (p.CollectionID == 0 || segment.CollectionID == p.CollectionID) &&
			(p.PartitionID == 0 || segment.PartitionID == p.PartitionID) &&
			(p.SegmentID == 0 || segment.ID == p.SegmentID) &&
			(p.State == "" || strings.EqualFold(segment.State.String(), p.State)) &&
			(p.Level == "" || strings.EqualFold(segment.Level.String(), p.Level))
	})
	if err != nil {
		fmt.Println("failed to list segments", err.Error())
		return nil
	}

	for _, segment := range segments {
		fmt.Print("Segment ID:", segment.ID)
		fmt.Printf("Json Stats: %v\n", segment.GetJsonKeyStats())
	}

	return nil
}
