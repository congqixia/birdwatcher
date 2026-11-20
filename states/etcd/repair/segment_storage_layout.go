package repair

import (
	"context"
	"fmt"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

const (
	storageVersionV0 int64 = 0
	storageVersionV2 int64 = 2
)

type RepairSegmentStorageLayoutParam struct {
	framework.ExecutionParam `use:"repair segment-storage-layout" desc:"repair segment storage version and FieldBinlog child fields"`
	SegmentID                int64    `name:"segment" default:"0" desc:"segment id to repair"`
	StorageVersion           int64    `name:"storageVersion" default:"-1" desc:"target storage version: 0 (Growing or Flushed, preserve legacy field binlogs) or 2 (Flushed only, repair column groups)"`
	ColumnGroups             []string `name:"columnGroup" desc:"required for version 2 only: complete mapping groupID:childID[:childID...]; repeat for every existing group"`
	AllowUnknownFields       bool     `name:"allowUnknownFields" default:"false" desc:"for version 2, allow child field ids not present in the current collection schema"`
}

type fieldBinlogRecord struct {
	key   string
	raw   string
	value *datapb.FieldBinlog
}

type segmentStorageLayoutChange struct {
	segment          *datapb.SegmentInfo
	fieldBinlogs     []*datapb.FieldBinlog
	fieldBinlogKeys  []string
	originalChildren map[int64][]int64
	changed          bool
}

// RepairSegmentStorageLayoutCommand repairs the metadata shape of an existing
// storage-v0 or storage-v2 segment. This command only changes metadata; callers
// must verify the physical binlog format separately before executing it.
func (c *ComponentRepair) RepairSegmentStorageLayoutCommand(ctx context.Context, p *RepairSegmentStorageLayoutParam) error {
	if p.SegmentID <= 0 {
		return fmt.Errorf("invalid segment id %d", p.SegmentID)
	}
	if p.StorageVersion != storageVersionV0 && p.StorageVersion != storageVersionV2 {
		return fmt.Errorf("unsupported target storage version %d: segment-storage-layout only supports versions 0 and 2", p.StorageVersion)
	}

	var columnGroups map[int64][]int64
	if p.StorageVersion == storageVersionV2 {
		var err error
		columnGroups, err = parseColumnGroupMappings(p.ColumnGroups)
		if err != nil {
			return err
		}
	} else if len(p.ColumnGroups) > 0 || p.AllowUnknownFields {
		return fmt.Errorf("--columnGroup and --allowUnknownFields are only supported for storage version 2")
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
	segment := segments[0]
	if p.StorageVersion == storageVersionV0 {
		if segment.GetState() != commonpb.SegmentState_Flushed && segment.GetState() != commonpb.SegmentState_Growing {
			return fmt.Errorf("segment %d state is %s, only Growing or Flushed segments can be repaired to storage version 0", segment.GetID(), segment.GetState())
		}
	} else if segment.GetState() != commonpb.SegmentState_Flushed {
		return fmt.Errorf("segment %d state is %s, only Flushed segments can be repaired", segment.GetID(), segment.GetState())
	}
	if segment.GetManifestPath() != "" {
		return fmt.Errorf("segment %d has manifest path %q and cannot be converted to storage version %d", segment.GetID(), segment.GetManifestPath(), p.StorageVersion)
	}

	segmentRaw, err := c.client.Load(ctx, segment.GetKey())
	if err != nil {
		return fmt.Errorf("failed to load raw segment value: %w", err)
	}

	fieldBinlogs, err := c.loadSegmentFieldBinlogs(ctx, segment)
	if err != nil {
		return err
	}
	if p.StorageVersion == storageVersionV2 && len(fieldBinlogs) == 0 {
		return fmt.Errorf("segment %d has no standalone FieldBinlog metadata", segment.GetID())
	}

	validFieldIDs := map[int64]struct{}{
		0: {},
		1: {},
	}
	if p.StorageVersion == storageVersionV2 && !p.AllowUnknownFields {
		collection, err := common.GetCollectionByIDVersion(ctx, c.client, c.basePath, segment.GetCollectionID())
		if err != nil {
			return fmt.Errorf("failed to load collection %d schema: %w", segment.GetCollectionID(), err)
		}
		validFieldIDs = collectionFieldIDs(collection)
	}

	change, err := prepareSegmentStorageLayout(
		segment.SegmentInfo,
		fieldBinlogs,
		p.StorageVersion,
		columnGroups,
		validFieldIDs,
		p.AllowUnknownFields,
	)
	if err != nil {
		return err
	}

	printSegmentStorageLayoutChange(segment, change)
	if !change.changed {
		fmt.Printf("segment %d storage layout already matches the requested metadata; no change\n", segment.GetID())
		return nil
	}
	if p.IsDryRun() {
		fmt.Println("dry-run mode, pass --run to actually execute")
		fmt.Println("warning: this command only repairs metadata; verify the physical files with inspect-parquet first")
		return nil
	}

	updateKeys := make([]string, 0, len(change.fieldBinlogs)+1)
	updateValues := make([]string, 0, len(change.fieldBinlogs)+1)
	updateKeys = append(updateKeys, segment.GetKey())
	segmentBytes, err := proto.Marshal(change.segment)
	if err != nil {
		return fmt.Errorf("failed to marshal updated segment: %w", err)
	}
	updateValues = append(updateValues, string(segmentBytes))
	for idx, fieldBinlog := range change.fieldBinlogs {
		bs, err := proto.Marshal(fieldBinlog)
		if err != nil {
			return fmt.Errorf("failed to marshal field binlog %d: %w", fieldBinlog.GetFieldID(), err)
		}
		updateKeys = append(updateKeys, change.fieldBinlogKeys[idx])
		updateValues = append(updateValues, string(bs))
	}

	originalKeys := make([]string, 0, len(fieldBinlogs)+1)
	originalValues := make([]string, 0, len(fieldBinlogs)+1)
	originalKeys = append(originalKeys, segment.GetKey())
	originalValues = append(originalValues, segmentRaw)
	if len(change.fieldBinlogs) > 0 {
		for _, record := range fieldBinlogs {
			originalKeys = append(originalKeys, record.key)
			originalValues = append(originalValues, record.raw)
		}
	}

	backupRoot := path.Join(
		"birdwatcher/backup/segment-storage-layout",
		time.Now().UTC().Format("20060102T150405.000000000Z"),
		strconv.FormatInt(segment.GetID(), 10),
	)
	backupKeys := make([]string, len(originalKeys))
	for idx, key := range originalKeys {
		backupKeys[idx] = path.Join(backupRoot, key)
	}
	if err := c.client.MultiSave(ctx, backupKeys, originalValues); err != nil {
		return fmt.Errorf("failed to back up original metadata: %w", err)
	}
	if err := c.client.MultiSave(ctx, updateKeys, updateValues); err != nil {
		return fmt.Errorf("failed to update segment storage layout; original metadata is stored under %s: %w", backupRoot, err)
	}

	fmt.Printf("segment %d storage layout updated successfully\n", segment.GetID())
	fmt.Printf("original metadata backup: %s\n", backupRoot)
	fmt.Println("restart the relevant Milvus components before serving traffic from this metadata")
	return nil
}

func (c *ComponentRepair) loadSegmentFieldBinlogs(ctx context.Context, segment *models.Segment) ([]fieldBinlogRecord, error) {
	prefix := path.Join(
		c.basePath,
		common.DCPrefix,
		"binlog",
		strconv.FormatInt(segment.GetCollectionID(), 10),
		strconv.FormatInt(segment.GetPartitionID(), 10),
		strconv.FormatInt(segment.GetID(), 10),
	) + "/"
	keys, values, err := c.client.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to load field binlogs for segment %d: %w", segment.GetID(), err)
	}
	if len(keys) != len(values) {
		return nil, fmt.Errorf("field binlog key/value count mismatch: %d keys, %d values", len(keys), len(values))
	}
	records := make([]fieldBinlogRecord, 0, len(keys))
	for idx, key := range keys {
		fieldBinlog := &datapb.FieldBinlog{}
		if err := proto.Unmarshal([]byte(values[idx]), fieldBinlog); err != nil {
			return nil, fmt.Errorf("failed to unmarshal field binlog %s: %w", key, err)
		}
		keyFieldID, err := strconv.ParseInt(path.Base(key), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid field binlog key %s: %w", key, err)
		}
		if keyFieldID != fieldBinlog.GetFieldID() {
			return nil, fmt.Errorf("field binlog key %s has field id %d but value contains field id %d", key, keyFieldID, fieldBinlog.GetFieldID())
		}
		records = append(records, fieldBinlogRecord{key: key, raw: values[idx], value: fieldBinlog})
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].value.GetFieldID() < records[j].value.GetFieldID()
	})
	return records, nil
}

func parseColumnGroupMappings(raw []string) (map[int64][]int64, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("at least one --columnGroup mapping is required")
	}

	result := make(map[int64][]int64, len(raw))
	for _, item := range raw {
		item = strings.TrimSpace(item)
		parts := strings.Split(item, ":")
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid column group mapping %q: expected groupID:childID[:childID...]", item)
		}
		groupID, err := parseNonNegativeID(parts[0], "column group")
		if err != nil {
			return nil, fmt.Errorf("invalid column group mapping %q: %w", item, err)
		}
		if _, exists := result[groupID]; exists {
			return nil, fmt.Errorf("column group %d is specified more than once", groupID)
		}

		children := make([]int64, 0, len(parts)-1)
		seen := make(map[int64]struct{}, len(parts)-1)
		for _, child := range parts[1:] {
			childID, err := parseNonNegativeID(child, "child field")
			if err != nil {
				return nil, fmt.Errorf("invalid column group mapping %q: %w", item, err)
			}
			if _, exists := seen[childID]; exists {
				return nil, fmt.Errorf("column group %d contains duplicate child field %d", groupID, childID)
			}
			seen[childID] = struct{}{}
			children = append(children, childID)
		}
		slices.Sort(children)
		result[groupID] = children
	}
	return result, nil
}

func parseNonNegativeID(raw, kind string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("%s id is empty", kind)
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s id %q", kind, raw)
	}
	if id < 0 {
		return 0, fmt.Errorf("%s id must be non-negative, got %d", kind, id)
	}
	return id, nil
}

func collectionFieldIDs(collection *models.Collection) map[int64]struct{} {
	result := map[int64]struct{}{
		0: {},
		1: {},
	}
	schema := collection.GetProto().GetSchema()
	for _, field := range schema.GetFields() {
		result[field.GetFieldID()] = struct{}{}
	}
	for _, structField := range schema.GetStructArrayFields() {
		result[structField.GetFieldID()] = struct{}{}
		for _, field := range structField.GetFields() {
			result[field.GetFieldID()] = struct{}{}
		}
	}
	return result
}

func prepareSegmentStorageLayout(
	segment *datapb.SegmentInfo,
	fieldBinlogs []fieldBinlogRecord,
	targetStorageVersion int64,
	columnGroups map[int64][]int64,
	validFieldIDs map[int64]struct{},
	allowUnknownFields bool,
) (*segmentStorageLayoutChange, error) {
	if targetStorageVersion == storageVersionV0 {
		// V0 uses per-field binlogs. Do not rewrite binlogs or infer a legacy
		// layout from column groups; the caller must verify the physical files.
		for _, fieldBinlog := range segment.GetBinlogs() {
			if len(fieldBinlog.GetChildFields()) > 0 {
				return nil, fmt.Errorf("embedded FieldBinlog %d has child fields; cannot set storage version 0", fieldBinlog.GetFieldID())
			}
		}
		for _, record := range fieldBinlogs {
			if len(record.value.GetChildFields()) > 0 {
				return nil, fmt.Errorf("FieldBinlog %d has child fields; cannot set storage version 0", record.value.GetFieldID())
			}
		}
		segmentClone := proto.Clone(segment).(*datapb.SegmentInfo)
		changed := segmentClone.GetStorageVersion() != storageVersionV0
		segmentClone.StorageVersion = storageVersionV0
		return &segmentStorageLayoutChange{segment: segmentClone, changed: changed}, nil
	}
	if len(fieldBinlogs) == 0 {
		return nil, fmt.Errorf("no FieldBinlog records supplied")
	}

	existing := make(map[int64]fieldBinlogRecord, len(fieldBinlogs))
	for _, record := range fieldBinlogs {
		fieldID := record.value.GetFieldID()
		if _, duplicate := existing[fieldID]; duplicate {
			return nil, fmt.Errorf("duplicate FieldBinlog metadata for column group %d", fieldID)
		}
		existing[fieldID] = record
	}

	missingGroups := make([]int64, 0)
	for groupID := range existing {
		if _, ok := columnGroups[groupID]; !ok {
			missingGroups = append(missingGroups, groupID)
		}
	}
	unknownGroups := make([]int64, 0)
	for groupID := range columnGroups {
		if _, ok := existing[groupID]; !ok {
			unknownGroups = append(unknownGroups, groupID)
		}
	}
	slices.Sort(missingGroups)
	slices.Sort(unknownGroups)
	if len(missingGroups) > 0 || len(unknownGroups) > 0 {
		return nil, fmt.Errorf("column group mapping must cover every existing FieldBinlog exactly once; missing=%v unknown=%v", missingGroups, unknownGroups)
	}

	childOwner := make(map[int64]int64)
	for groupID, children := range columnGroups {
		if len(children) == 0 {
			return nil, fmt.Errorf("column group %d has no child fields", groupID)
		}
		for _, childID := range children {
			if !allowUnknownFields {
				if _, ok := validFieldIDs[childID]; !ok {
					return nil, fmt.Errorf("column group %d references child field %d which is not present in the current collection schema", groupID, childID)
				}
			}
			if owner, duplicate := childOwner[childID]; duplicate {
				return nil, fmt.Errorf("child field %d belongs to both column group %d and %d", childID, owner, groupID)
			}
			childOwner[childID] = groupID
		}
	}

	segmentClone := proto.Clone(segment).(*datapb.SegmentInfo)
	changed := segmentClone.GetStorageVersion() != targetStorageVersion
	segmentClone.StorageVersion = targetStorageVersion

	fieldClones := make([]*datapb.FieldBinlog, 0, len(fieldBinlogs))
	fieldKeys := make([]string, 0, len(fieldBinlogs))
	originalChildren := make(map[int64][]int64, len(fieldBinlogs))
	for _, record := range fieldBinlogs {
		clone := proto.Clone(record.value).(*datapb.FieldBinlog)
		oldChildren := append([]int64(nil), clone.GetChildFields()...)
		newChildren := append([]int64(nil), columnGroups[clone.GetFieldID()]...)
		slices.Sort(oldChildren)
		slices.Sort(newChildren)
		originalChildren[clone.GetFieldID()] = oldChildren
		if !slices.Equal(oldChildren, newChildren) {
			changed = true
		}
		clone.ChildFields = newChildren
		fieldClones = append(fieldClones, clone)
		fieldKeys = append(fieldKeys, record.key)
	}
	if changed {
		segmentClone.DataVersion++
	}

	return &segmentStorageLayoutChange{
		segment:          segmentClone,
		fieldBinlogs:     fieldClones,
		fieldBinlogKeys:  fieldKeys,
		originalChildren: originalChildren,
		changed:          changed,
	}, nil
}

func printSegmentStorageLayoutChange(segment *models.Segment, change *segmentStorageLayoutChange) {
	fmt.Printf("segment %d storage version: %d -> %d\n", segment.GetID(), segment.GetStorageVersion(), change.segment.GetStorageVersion())
	fmt.Printf("segment %d data version: %d -> %d\n", segment.GetID(), segment.GetDataVersion(), change.segment.GetDataVersion())
	for _, fieldBinlog := range change.fieldBinlogs {
		groupID := fieldBinlog.GetFieldID()
		fmt.Printf("column group %d child fields: %v -> %v\n", groupID, change.originalChildren[groupID], fieldBinlog.GetChildFields())
	}
}
