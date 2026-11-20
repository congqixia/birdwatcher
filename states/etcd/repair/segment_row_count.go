package repair

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type RepairSegmentRowCountParam struct {
	framework.ExecutionParam `use:"repair segment-row-count" desc:"repair only the NumOfRows field of a segment, preserving all other metadata"`
	SegmentID                int64 `name:"segment" default:"0" desc:"segment id to repair (required)"`
	RowCount                 int64 `name:"rowCount" default:"-1" desc:"target row count, must be non-negative (required)"`
}

func (c *ComponentRepair) RepairSegmentRowCountCommand(ctx context.Context, p *RepairSegmentRowCountParam) error {
	if p.SegmentID <= 0 {
		return fmt.Errorf("invalid segment id %d: --segment must be positive", p.SegmentID)
	}
	if p.RowCount < 0 {
		return fmt.Errorf("invalid row count %d: --rowCount must be specified and non-negative", p.RowCount)
	}
	segments, err := common.ListSegmentsBy(ctx, c.client, c.basePath, common.SegmentSelector{SegmentID: p.SegmentID})
	if err != nil {
		return fmt.Errorf("failed to load segment %d: %w", p.SegmentID, err)
	}
	if len(segments) == 0 {
		return fmt.Errorf("segment %d not found", p.SegmentID)
	}
	if len(segments) != 1 {
		return fmt.Errorf("found %d segment records with id %d", len(segments), p.SegmentID)
	}

	key := segments[0].GetKey()
	raw, err := c.client.Load(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to load raw segment metadata: %w", err)
	}
	// Decode the exact record being backed up, without enriching its binlogs.
	segment := &datapb.SegmentInfo{}
	if err := proto.Unmarshal([]byte(raw), segment); err != nil {
		return fmt.Errorf("failed to decode segment %d: %w", p.SegmentID, err)
	}
	expectedKey := path.Join(c.basePath, common.DCPrefix, common.SegmentMetaPrefix,
		strconv.FormatInt(segment.GetCollectionID(), 10), strconv.FormatInt(segment.GetPartitionID(), 10), strconv.FormatInt(p.SegmentID, 10))
	if segment.GetID() != p.SegmentID || key != expectedKey {
		return fmt.Errorf("segment metadata does not match its key: %s", key)
	}

	fmt.Printf("segment %d row count: %d -> %d\n", p.SegmentID, segment.GetNumOfRows(), p.RowCount)
	if segment.GetNumOfRows() == p.RowCount {
		fmt.Println("segment row count already matches; no change")
		return nil
	}
	if p.IsDryRun() {
		fmt.Println("dry-run mode, pass --run to actually execute")
		return nil
	}

	segment.NumOfRows = p.RowCount
	updated, err := proto.Marshal(segment)
	if err != nil {
		return fmt.Errorf("failed to marshal segment %d: %w", p.SegmentID, err)
	}
	backupKey := path.Join("birdwatcher/backup/segment-row-count", time.Now().UTC().Format("20060102T150405.000000000Z"), key)
	if err := c.client.Save(ctx, backupKey, raw); err != nil {
		return fmt.Errorf("failed to back up original segment metadata: %w", err)
	}
	if err := c.client.Save(ctx, key, string(updated)); err != nil {
		return fmt.Errorf("failed to update segment row count; original metadata is stored at %s: %w", backupKey, err)
	}
	fmt.Printf("segment %d row count updated successfully; original metadata backup: %s\n", p.SegmentID, backupKey)
	return nil
}
