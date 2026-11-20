package repair

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type RepairSegmentStateParam struct {
	framework.ExecutionParam `use:"repair segment-state" desc:"repair only the State field of a segment, preserving all other metadata"`
	SegmentID                int64  `name:"segment" default:"0" desc:"segment id to repair (required)"`
	State                    string `name:"state" default:"" desc:"target SegmentState enum name (case-insensitive) or numeric value (required)" values:"SegmentStateNone,NotExist,Growing,Sealed,Flushed,Flushing,Dropped,Importing"`
}

func parseSegmentState(raw string) (commonpb.SegmentState, error) {
	value := strings.TrimSpace(raw)
	for name, number := range commonpb.SegmentState_value {
		if strings.EqualFold(value, name) {
			return commonpb.SegmentState(number), nil
		}
	}
	if number, err := strconv.ParseInt(value, 10, 32); err == nil {
		if _, ok := commonpb.SegmentState_name[int32(number)]; ok {
			return commonpb.SegmentState(number), nil
		}
	}
	valid := make([]string, 0, len(commonpb.SegmentState_value))
	for name, number := range commonpb.SegmentState_value {
		valid = append(valid, fmt.Sprintf("%s(%d)", name, number))
	}
	sort.Strings(valid)
	return 0, fmt.Errorf("invalid segment state %q; valid states: %s", raw, strings.Join(valid, ", "))
}

func (c *ComponentRepair) RepairSegmentStateCommand(ctx context.Context, p *RepairSegmentStateParam) error {
	if p.SegmentID <= 0 {
		return fmt.Errorf("invalid segment id %d: --segment must be positive", p.SegmentID)
	}
	target, err := parseSegmentState(p.State)
	if err != nil {
		return err
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

	fmt.Printf("segment %d state: %s -> %s\n", p.SegmentID, segment.GetState(), target)
	if segment.GetState() == target {
		fmt.Println("segment state already matches; no change")
		return nil
	}
	if p.IsDryRun() {
		fmt.Println("dry-run mode, pass --run to actually execute")
		return nil
	}

	segment.State = target
	updated, err := proto.Marshal(segment)
	if err != nil {
		return fmt.Errorf("failed to marshal segment %d: %w", p.SegmentID, err)
	}
	backupKey := path.Join("birdwatcher/backup/segment-state", time.Now().UTC().Format("20060102T150405.000000000Z"), key)
	if err := c.client.Save(ctx, backupKey, raw); err != nil {
		return fmt.Errorf("failed to back up original segment metadata: %w", err)
	}
	if err := c.client.Save(ctx, key, string(updated)); err != nil {
		return fmt.Errorf("failed to update segment state; original metadata is stored at %s: %w", backupKey, err)
	}
	fmt.Printf("segment %d state updated successfully; original metadata backup: %s\n", p.SegmentID, backupKey)
	return nil
}
