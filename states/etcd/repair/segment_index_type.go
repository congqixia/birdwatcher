package repair

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type SegmentIndexTypeParam struct {
	framework.ExecutionParam `use:"repair segment-index-type" desc:"set the operator-specified actual type of finished etcd segment indexes with per-record CAS; stop metadata writers first, restart DataCoord and reload the collection afterward; live etcd only"`
	CollectionID             int64  `name:"collection" default:"0" desc:"collection ID (required)"`
	IndexID                  int64  `name:"index" default:"0" desc:"index ID (required)"`
	SegmentID                int64  `name:"segment" default:"0" desc:"restrict to one segment; default is all live segments"`
	IndexType                string `name:"index-type" desc:"actual artifact type, e.g. HNSW; caller must verify all selected artifacts use this type"`
}

var actualIndexTypePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

func (c *ComponentRepair) SegmentIndexTypeCommand(ctx context.Context, p *SegmentIndexTypeParam) error {
	if p.CollectionID <= 0 || p.IndexID <= 0 || p.SegmentID < 0 ||
		!actualIndexTypePattern.MatchString(p.IndexType) || p.IndexType == "AUTOINDEX" {
		return fmt.Errorf("positive --collection and --index, nonnegative --segment and a concrete uppercase --index-type (not AUTOINDEX) are required")
	}
	collection := strconv.FormatInt(p.CollectionID, 10)
	indexPrefix := path.Join(c.basePath, common.IndexPrefix, collection) + "/"
	segmentPrefix := path.Join(c.basePath, common.DCPrefix, common.SegmentMetaPrefix, collection) + "/"
	buildPrefix := path.Join(c.basePath, common.SegmentIndexPrefix, collection) + "/"
	snapshot, err := kv.ReadEtcdRepairSnapshot(ctx, c.client, []string{indexPrefix, segmentPrefix, buildPrefix})
	if err != nil {
		return err
	}
	dependencies, updates, preview, err := planSegmentIndexTypeRepair(snapshot, p, indexPrefix, segmentPrefix, buildPrefix)
	if err != nil {
		return err
	}
	fmt.Print(preview)
	fmt.Printf("%d index records would change. Type is asserted by the caller; index files are not inspected.\n", len(updates))
	if len(updates) == 0 {
		return nil
	}
	if !p.Run {
		fmt.Println("Dry run: nothing written. Stop metadata writers, then rerun with --run.")
		return nil
	}
	committed, err := kv.ApplyEtcdRepair(ctx, c.client, snapshot, dependencies, updates)
	if err != nil {
		return fmt.Errorf("repair stopped after %d/%d confirmed commits (no rollback): %w", committed, len(updates), err)
	}
	fmt.Println("All planned records updated and readback verified. Restart DataCoord and reload the affected collection before serving queries.")
	return nil
}

func planSegmentIndexTypeRepair(snapshot *kv.EtcdRepairSnapshot, p *SegmentIndexTypeParam, indexPrefix, segmentPrefix, buildPrefix string) (map[string][]string, map[string]string, string, error) {
	indexKey := indexPrefix + strconv.FormatInt(p.IndexID, 10)
	entry, ok := snapshot.Entries[indexKey]
	if !ok {
		return nil, nil, "", fmt.Errorf("index %d does not exist in collection %d", p.IndexID, p.CollectionID)
	}
	definition := &indexpb.FieldIndex{}
	if err := proto.Unmarshal(entry.Value, definition); err != nil {
		return nil, nil, "", fmt.Errorf("decode index definition %s: %w", indexKey, err)
	}
	if definition.GetDeleted() || definition.GetIndexInfo().GetCollectionID() != p.CollectionID || definition.GetIndexInfo().GetIndexID() != p.IndexID {
		return nil, nil, "", fmt.Errorf("index definition is deleted or does not match its key: %s", indexKey)
	}
	dependencies := make(map[string][]string)
	segmentKeys := make(map[int64]string)
	segments := make(map[int64]*datapb.SegmentInfo)
	keys := make([]string, 0, len(snapshot.Entries))
	for key := range snapshot.Entries {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if !strings.HasPrefix(key, segmentPrefix) {
			continue
		}
		segment := &datapb.SegmentInfo{}
		if err := proto.Unmarshal(snapshot.Entries[key].Value, segment); err != nil {
			return nil, nil, "", fmt.Errorf("decode segment %s: %w", key, err)
		}
		expected := fmt.Sprintf("%s%d/%d", segmentPrefix, segment.GetPartitionID(), segment.GetID())
		if segment.GetCollectionID() != p.CollectionID || segment.GetID() <= 0 || expected != key {
			return nil, nil, "", fmt.Errorf("segment metadata does not match its key: %s", key)
		}
		if p.SegmentID != 0 && p.SegmentID != segment.GetID() {
			continue
		}
		if _, exists := segments[segment.GetID()]; exists {
			return nil, nil, "", fmt.Errorf("duplicate segment %d", segment.GetID())
		}
		segments[segment.GetID()] = segment
		if segment.GetState() == commonpb.SegmentState_Dropped {
			continue
		}
		if segment.GetManifestHasIndex() {
			return nil, nil, "", fmt.Errorf("segment %d has manifest indexes; etcd-only repair refused", segment.GetID())
		}
		segmentKeys[segment.GetID()] = key
	}
	if p.SegmentID != 0 && (segments[p.SegmentID] == nil || segments[p.SegmentID].GetState() == commonpb.SegmentState_Dropped) {
		return nil, nil, "", fmt.Errorf("segment %d is absent or dropped", p.SegmentID)
	}
	updates := make(map[string]string)
	var preview strings.Builder
	selected := 0
	for _, key := range keys {
		if !strings.HasPrefix(key, buildPrefix) {
			continue
		}
		index := &indexpb.SegmentIndex{}
		if err := proto.Unmarshal(snapshot.Entries[key].Value, index); err != nil {
			return nil, nil, "", fmt.Errorf("decode segment index %s: %w", key, err)
		}
		expected := fmt.Sprintf("%s%d/%d/%d", buildPrefix, index.GetPartitionID(), index.GetSegmentID(), index.GetBuildID())
		if index.GetCollectionID() != p.CollectionID || index.GetBuildID() <= 0 || expected != key {
			return nil, nil, "", fmt.Errorf("segment index metadata does not match its key: %s", key)
		}
		if index.GetIndexID() != p.IndexID || index.GetDeleted() || index.GetState() != commonpb.IndexState_Finished || len(index.GetIndexFileKeys()) == 0 ||
			p.SegmentID != 0 && p.SegmentID != index.GetSegmentID() {
			continue
		}
		segment := segments[index.GetSegmentID()]
		if segment == nil {
			return nil, nil, "", fmt.Errorf("build %d references missing segment %d", index.GetBuildID(), index.GetSegmentID())
		}
		if segment.GetState() == commonpb.SegmentState_Dropped {
			continue
		}
		if segment.GetPartitionID() != index.GetPartitionID() {
			return nil, nil, "", fmt.Errorf("partition mismatch for build %d", index.GetBuildID())
		}
		if segment.GetState() != commonpb.SegmentState_Flushed {
			return nil, nil, "", fmt.Errorf("segment %d is not Flushed; stop ongoing restore/build work first", segment.GetID())
		}
		if index.GetIndexStorePathVersion() != indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED && index.GetIndexStorePathVersion() != indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED {
			return nil, nil, "", fmt.Errorf("build %d is manifest-owned or has an unsupported path version", index.GetBuildID())
		}
		selected++
		if index.GetIndexType() == p.IndexType {
			continue
		}
		fmt.Fprintf(&preview, "segment=%d index=%d build=%d type=%q -> %q\n", index.GetSegmentID(), index.GetIndexID(), index.GetBuildID(), index.GetIndexType(), p.IndexType)
		// Mutate the decoded message to retain every other field, including unknown fields.
		index.IndexType = p.IndexType
		value, err := proto.Marshal(index)
		if err != nil {
			return nil, nil, "", fmt.Errorf("encode segment index %s: %w", key, err)
		}
		updates[key] = string(value)
		dependencies[key] = []string{indexKey, segmentKeys[index.GetSegmentID()]}
	}
	if selected == 0 {
		return nil, nil, "", fmt.Errorf("no eligible finished segment index artifacts found")
	}
	return dependencies, updates, preview.String(), nil
}
