package states

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type ScanV3IndexParams struct {
	framework.DataSetParam `use:"scan-v3-index" desc:"scan scalar index V3 files and diagnose schema/path/meta mismatches" alias:"scan-scalar-index-v3"`
	OSSOverrideParam
	CollectionID           int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	PartitionID            int64  `name:"partition" default:"0" desc:"partition id to filter with"`
	SegmentID              int64  `name:"segment" default:"0" desc:"segment id to filter with"`
	FieldID                int64  `name:"field" default:"0" desc:"field id to filter with"`
	IndexID                int64  `name:"indexID" default:"0" desc:"collection index id to filter with"`
	BuildID                int64  `name:"buildID" default:"0" desc:"index build id to filter with"`
	MinioAddress           string `name:"minioAddr" default:"" desc:"override minio address"`
	SkipBucketCheck        bool   `name:"skipBucketCheck" default:"false" desc:"skip bucket existence check due to permission restrictions"`
	WorkerNum              int64  `name:"workerNum" default:"8" desc:"concurrent range-read workers"`
	OnlyIssues             bool   `name:"onlyIssues" default:"true" desc:"only output objects with findings"`
	IncludeDeleted         bool   `name:"includeDeleted" default:"false" desc:"include deleted SegmentIndex metadata"`
}

type V3IndexScanRecord struct {
	Bucket                     string           `json:"bucket"`
	ObjectKey                  string           `json:"object_key"`
	ObjectSize                 int64            `json:"object_size"`
	ETag                       string           `json:"etag,omitempty"`
	LastModified               string           `json:"last_modified,omitempty"`
	VersionID                  string           `json:"version_id,omitempty"`
	Layout                     string           `json:"layout"`
	PathCollectionID           int64            `json:"path_collection_id,omitempty"`
	PathBuildID                int64            `json:"path_build_id,omitempty"`
	PathIndexVersion           int64            `json:"path_index_version,omitempty"`
	PathPartitionID            int64            `json:"path_partition_id,omitempty"`
	PathSegmentID              int64            `json:"path_segment_id,omitempty"`
	MetadataCollectionID       int64            `json:"metadata_collection_id,omitempty"`
	MetadataPartitionID        int64            `json:"metadata_partition_id,omitempty"`
	MetadataSegmentID          int64            `json:"metadata_segment_id,omitempty"`
	MetadataFieldID            int64            `json:"metadata_field_id,omitempty"`
	MetadataFieldName          string           `json:"metadata_field_name,omitempty"`
	MetadataFieldType          string           `json:"metadata_field_type,omitempty"`
	EffectiveFieldType         string           `json:"effective_field_type,omitempty"`
	JSONPath                   string           `json:"json_path,omitempty"`
	JSONCastType               string           `json:"json_cast_type,omitempty"`
	IsJSONPathIndex            bool             `json:"is_json_path_index,omitempty"`
	MetadataIndexID            int64            `json:"metadata_index_id,omitempty"`
	MetadataIndexType          string           `json:"metadata_index_type,omitempty"`
	MetadataIndexVersion       int64            `json:"metadata_index_version,omitempty"`
	MetadataScalarIndexVersion int32            `json:"metadata_scalar_index_version,omitempty"`
	IndexStorePathVersion      int32            `json:"index_store_path_version"`
	FileFormatVersion          uint16           `json:"file_format_version,omitempty"`
	Encrypted                  bool             `json:"encrypted"`
	EncryptionZone             string           `json:"encryption_zone,omitempty"`
	DirectoryEntries           []string         `json:"directory_entries,omitempty"`
	Meta                       map[string]any   `json:"meta,omitempty"`
	PhysicalIndexType          string           `json:"physical_index_type,omitempty"`
	ExpectedLoader             string           `json:"expected_loader,omitempty"`
	IndexDataSize              int64            `json:"index_data_size,omitempty"`
	BytesPerEntry              float64          `json:"bytes_per_entry,omitempty"`
	AdvertisedPath             string           `json:"advertised_path,omitempty"`
	ResolvedPath               string           `json:"resolved_path,omitempty"`
	Findings                   []V3IndexFinding `json:"findings"`

	effectiveType      schemapb.DataType
	effectiveTypeKnown bool
	isJSONPathIndex    bool
}

type V3IndexScanSummary struct {
	ScannedObjects  int            `json:"scanned_objects"`
	ReportedObjects int            `json:"reported_objects"`
	FindingCounts   map[string]int `json:"finding_counts"`
}

type V3IndexScanReport struct {
	Summary V3IndexScanSummary   `json:"summary"`
	Records []*V3IndexScanRecord `json:"records"`
}

func (r *V3IndexScanReport) Entities() any {
	return r
}

func (r *V3IndexScanReport) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatJSON:
		return framework.MarshalJSON(r)
	case framework.FormatLine:
		var sb strings.Builder
		for _, record := range r.Records {
			data, err := json.Marshal(record)
			if err != nil {
				continue
			}
			sb.Write(data)
			sb.WriteByte('\n')
		}
		return strings.TrimSuffix(sb.String(), "\n")
	case framework.FormatDefault, framework.FormatPlain:
		return r.printPlain()
	default:
		return r.printPlain()
	}
}

func (r *V3IndexScanReport) TableHeaders() table.Row {
	return table.Row{"Object", "Collection", "Field", "Effective Type", "Physical Type", "Findings"}
}

func (r *V3IndexScanReport) TableRows() []table.Row {
	rows := make([]table.Row, 0, len(r.Records))
	for _, record := range r.Records {
		rules := make([]string, 0, len(record.Findings))
		for _, finding := range record.Findings {
			rules = append(rules, finding.Rule)
		}
		rows = append(rows, table.Row{
			record.ObjectKey,
			record.MetadataCollectionID,
			fmt.Sprintf("%s(%d)", record.MetadataFieldName, record.MetadataFieldID),
			record.EffectiveFieldType,
			record.PhysicalIndexType,
			strings.Join(rules, ","),
		})
	}
	return rows
}

func (r *V3IndexScanReport) TableTitle() string {
	return fmt.Sprintf("V3 scalar indexes: scanned=%d reported=%d", r.Summary.ScannedObjects, r.Summary.ReportedObjects)
}

func (r *V3IndexScanReport) printPlain() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Scanned %d V3 index objects; reported %d objects with the selected output policy.\n", r.Summary.ScannedObjects, r.Summary.ReportedObjects)
	if len(r.Summary.FindingCounts) > 0 {
		rules := make([]string, 0, len(r.Summary.FindingCounts))
		for rule := range r.Summary.FindingCounts {
			rules = append(rules, rule)
		}
		sort.Strings(rules)
		for _, rule := range rules {
			fmt.Fprintf(&sb, "  %s: %d\n", rule, r.Summary.FindingCounts[rule])
		}
	}
	for _, record := range r.Records {
		fmt.Fprintf(&sb, "\n%s\n", record.ObjectKey)
		fmt.Fprintf(&sb, "  build=%d collection=%d segment=%d field=%s(%d) effective=%s physical=%s\n",
			record.PathBuildID, record.MetadataCollectionID, record.MetadataSegmentID,
			record.MetadataFieldName, record.MetadataFieldID, record.EffectiveFieldType, record.PhysicalIndexType)
		if len(record.Findings) == 0 {
			fmt.Fprintln(&sb, "  [OK] no findings")
			continue
		}
		for _, finding := range record.Findings {
			fmt.Fprintf(&sb, "  [%s] %s: %s\n", finding.Severity, finding.Rule, finding.Message)
		}
	}
	return strings.TrimSuffix(sb.String(), "\n")
}

type v3ScanMetadata struct {
	segmentIndexesByBuild map[int64][]*indexpb.SegmentIndex
	fieldIndexes          map[v3IndexKey]*indexpb.IndexInfo
	collections           map[int64]*models.Collection
	segments              map[int64]*models.Segment
}

type v3IndexKey struct {
	collectionID int64
	indexID      int64
}

type v3ObjectPath struct {
	layout       string
	collectionID int64
	buildID      int64
	indexVersion int64
	partitionID  int64
	segmentID    int64
	filename     string
}

func (s *InstanceState) ScanV3IndexCommand(ctx context.Context, p *ScanV3IndexParams) (*framework.PresetResultSet, error) {
	if p.WorkerNum <= 0 {
		return nil, fmt.Errorf("workerNum must be greater than zero")
	}

	metadata, err := s.loadV3ScanMetadata(ctx, p.IncludeDeleted)
	if err != nil {
		return nil, err
	}
	resolvedStore, err := s.getObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, p.SkipBucketCheck)
	if err != nil {
		return nil, err
	}

	objects, err := listV3IndexObjects(ctx, resolvedStore.Store, resolvedStore.RootPath)
	if err != nil {
		return nil, err
	}
	reader := objectStoreV3RangeReader{store: resolvedStore.Store}
	type scanResult struct {
		record  *V3IndexScanRecord
		matched bool
	}
	jobs := make(chan oss.ObjectInfo)
	var wg sync.WaitGroup
	workerCount := p.WorkerNum
	if workerCount > int64(len(objects)) {
		workerCount = int64(len(objects))
	}
	results := make(chan scanResult, int(workerCount))
	for i := 0; int64(i) < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for object := range jobs {
				record, matched := scanV3IndexObject(ctx, resolvedStore, reader, metadata, object, p)
				results <- scanResult{record: record, matched: matched}
			}
		}()
	}
	go func() {
		for _, object := range objects {
			jobs <- object
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	report := &V3IndexScanReport{
		Summary: V3IndexScanSummary{FindingCounts: make(map[string]int)},
		Records: make([]*V3IndexScanRecord, 0),
	}
	for result := range results {
		if !result.matched || result.record == nil {
			continue
		}
		report.Summary.ScannedObjects++
		for _, finding := range result.record.Findings {
			report.Summary.FindingCounts[finding.Rule]++
		}
		if p.OnlyIssues && len(result.record.Findings) == 0 {
			continue
		}
		report.Records = append(report.Records, result.record)
	}
	sort.Slice(report.Records, func(i, j int) bool {
		return report.Records[i].ObjectKey < report.Records[j].ObjectKey
	})
	report.Summary.ReportedObjects = len(report.Records)

	return framework.NewPresetResultSet(report, framework.NameFormat(p.Format)), nil
}

func (s *InstanceState) loadV3ScanMetadata(ctx context.Context, includeDeleted bool) (*v3ScanMetadata, error) {
	segmentIndexes, err := common.ListSegmentIndex(ctx, s.client, s.basePath, func(index *models.SegmentIndex) bool {
		return includeDeleted || !index.GetProto().GetDeleted()
	})
	if err != nil {
		return nil, err
	}
	fieldIndexes, err := common.ListIndex(ctx, s.client, s.basePath)
	if err != nil {
		return nil, err
	}
	collections, err := common.ListCollections(ctx, s.client, s.basePath)
	if err != nil {
		return nil, err
	}
	segments, err := common.ListSegments(ctx, s.client, s.basePath)
	if err != nil {
		return nil, err
	}

	metadata := &v3ScanMetadata{
		segmentIndexesByBuild: make(map[int64][]*indexpb.SegmentIndex),
		fieldIndexes:          make(map[v3IndexKey]*indexpb.IndexInfo),
		collections:           make(map[int64]*models.Collection),
		segments:              make(map[int64]*models.Segment),
	}
	for _, index := range segmentIndexes {
		proto := index.GetProto()
		metadata.segmentIndexesByBuild[proto.GetBuildID()] = append(metadata.segmentIndexesByBuild[proto.GetBuildID()], proto)
	}
	for _, index := range fieldIndexes {
		info := index.GetProto().GetIndexInfo()
		if info == nil {
			continue
		}
		metadata.fieldIndexes[v3IndexKey{collectionID: info.GetCollectionID(), indexID: info.GetIndexID()}] = info
	}
	for _, collection := range collections {
		metadata.collections[collection.GetProto().GetID()] = collection
	}
	for _, segment := range segments {
		metadata.segments[segment.GetID()] = segment
	}
	return metadata, nil
}

func listV3IndexObjects(ctx context.Context, store oss.ObjectStore, rootPath string) ([]oss.ObjectInfo, error) {
	prefixes := []string{
		path.Join(rootPath, "index_files"),
		path.Join(rootPath, "index_v1"),
	}
	seen := make(map[string]struct{})
	objects := make([]oss.ObjectInfo, 0)
	for _, prefix := range prefixes {
		stream, err := store.List(ctx, prefix, true)
		if err != nil {
			return nil, err
		}
		for object := range stream {
			if object.Err != nil {
				return nil, object.Err
			}
			if object.IsDir || !strings.HasSuffix(strings.ToLower(object.Key), ".v3") {
				continue
			}
			if _, ok := seen[object.Key]; ok {
				continue
			}
			seen[object.Key] = struct{}{}
			objects = append(objects, object)
		}
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects, nil
}

func scanV3IndexObject(ctx context.Context, resolvedStore *oss.ResolvedObjectStore, reader v3RangeReader, metadata *v3ScanMetadata, object oss.ObjectInfo, p *ScanV3IndexParams) (*V3IndexScanRecord, bool) {
	parsedPath, pathErr := parseV3ObjectPath(resolvedStore.RootPath, object.Key)
	if p.BuildID != 0 && (pathErr != nil || parsedPath.buildID != p.BuildID) {
		return nil, false
	}
	candidates := metadata.segmentIndexesByBuild[parsedPath.buildID]
	segmentIndex, ambiguous := chooseV3SegmentIndex(parsedPath, candidates)
	fieldIndex := v3FieldIndexForSegment(metadata, segmentIndex)
	if !matchesV3ScanFilters(parsedPath, segmentIndex, fieldIndex, p) {
		return nil, false
	}

	record := &V3IndexScanRecord{
		Bucket:           resolvedStore.BucketName,
		ObjectKey:        object.Key,
		ObjectSize:       object.Size,
		ETag:             object.ETag,
		VersionID:        object.VersionID,
		Layout:           parsedPath.layout,
		PathCollectionID: parsedPath.collectionID,
		PathBuildID:      parsedPath.buildID,
		PathIndexVersion: parsedPath.indexVersion,
		PathPartitionID:  parsedPath.partitionID,
		PathSegmentID:    parsedPath.segmentID,
		Findings:         make([]V3IndexFinding, 0),
	}
	if !object.LastModified.IsZero() {
		record.LastModified = object.LastModified.UTC().Format(time.RFC3339Nano)
	}
	if pathErr != nil {
		record.Layout = "UNKNOWN"
		addV3Finding(record, "INDEX_PATH_INVALID", v3SeverityCritical, pathErr.Error())
	}
	if len(candidates) == 0 {
		addV3Finding(record, "BUILD_ID_NOT_FOUND", v3SeverityCritical,
			fmt.Sprintf("path build ID %d has no SegmentIndex metadata", parsedPath.buildID))
	}
	if ambiguous {
		addV3Finding(record, "BUILD_ID_AMBIGUOUS", v3SeverityCritical,
			fmt.Sprintf("build ID %d is associated with multiple SegmentIndex records", parsedPath.buildID))
	}
	if segmentIndex != nil {
		populateV3Metadata(record, metadata, segmentIndex, fieldIndex, parsedPath.filename, resolvedStore.RootPath)
		checkV3PathMetadata(record, parsedPath, segmentIndex)
	}

	inspection, inspectionFindings := inspectV3Index(ctx, reader, object.Key, object.Size)
	record.FileFormatVersion = inspection.FormatVersion
	record.Encrypted = inspection.Encrypted
	record.EncryptionZone = inspection.EncryptionZone
	record.Meta = inspection.Meta
	record.IndexDataSize = inspection.IndexDataSize
	record.BytesPerEntry = inspection.BytesPerEntry
	for _, entry := range inspection.Entries {
		record.DirectoryEntries = append(record.DirectoryEntries, entry.Name)
	}
	for _, finding := range inspectionFindings {
		addV3Finding(record, finding.Rule, finding.Severity, finding.Message)
	}
	if inspection.Meta != nil {
		record.PhysicalIndexType = classifyV3PhysicalIndex(inspection.Meta, record.MetadataIndexType)
		checkV3MetaCompatibility(record, segmentIndex, metadata.segments[record.MetadataSegmentID])
		checkV3Filename(record, parsedPath.filename)
	}

	if stat, err := resolvedStore.Store.Stat(ctx, object.Key); err == nil {
		if objectChangedSinceList(object, stat) {
			addV3Finding(record, "OBJECT_VERSION_CHANGED", v3SeverityWarning,
				"object ETag, version ID, size, or last-modified value changed while it was being scanned")
		}
	}
	sort.SliceStable(record.Findings, func(i, j int) bool {
		left := v3SeverityRank(record.Findings[i].Severity)
		right := v3SeverityRank(record.Findings[j].Severity)
		if left != right {
			return left < right
		}
		return record.Findings[i].Rule < record.Findings[j].Rule
	})
	return record, true
}

func parseV3ObjectPath(rootPath, objectKey string) (v3ObjectPath, error) {
	result := v3ObjectPath{filename: path.Base(objectKey)}
	cleaned := strings.TrimPrefix(path.Clean(objectKey), "/")
	root := strings.Trim(path.Clean(rootPath), "/")
	if root != "" && root != "." {
		cleaned = strings.TrimPrefix(cleaned, root+"/")
	}
	parts := strings.Split(cleaned, "/")
	marker := -1
	for i, part := range parts {
		if part == "index_files" || part == "index_v1" {
			marker = i
			break
		}
	}
	if marker < 0 {
		return result, fmt.Errorf("object key %q is not under index_files or index_v1", objectKey)
	}
	parts = parts[marker:]
	parseID := func(position int, name string) (int64, error) {
		if position >= len(parts) {
			return 0, fmt.Errorf("missing %s in object key %q", name, objectKey)
		}
		value, err := strconv.ParseInt(parts[position], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid %s %q in object key %q", name, parts[position], objectKey)
		}
		return value, nil
	}
	var err error
	if parts[0] == "index_files" {
		result.layout = "BUILD_ROOTED"
		if len(parts) != 6 {
			return result, fmt.Errorf("build-rooted V3 key must contain 6 path components after root, got %d in %q", len(parts), objectKey)
		}
		if result.buildID, err = parseID(1, "build ID"); err != nil {
			return result, err
		}
		if result.indexVersion, err = parseID(2, "index version"); err != nil {
			return result, err
		}
		if result.partitionID, err = parseID(3, "partition ID"); err != nil {
			return result, err
		}
		if result.segmentID, err = parseID(4, "segment ID"); err != nil {
			return result, err
		}
		result.filename = parts[5]
		return result, nil
	}

	result.layout = "COLLECTION_ROOTED"
	if len(parts) != 7 {
		return result, fmt.Errorf("collection-rooted V3 key must contain 7 path components after root, got %d in %q", len(parts), objectKey)
	}
	if result.collectionID, err = parseID(1, "collection ID"); err != nil {
		return result, err
	}
	if result.partitionID, err = parseID(2, "partition ID"); err != nil {
		return result, err
	}
	if result.segmentID, err = parseID(3, "segment ID"); err != nil {
		return result, err
	}
	if result.buildID, err = parseID(4, "build ID"); err != nil {
		return result, err
	}
	if result.indexVersion, err = parseID(5, "index version"); err != nil {
		return result, err
	}
	result.filename = parts[6]
	return result, nil
}

func chooseV3SegmentIndex(objectPath v3ObjectPath, candidates []*indexpb.SegmentIndex) (*indexpb.SegmentIndex, bool) {
	if len(candidates) == 0 {
		return nil, false
	}
	unique := make(map[string]struct{})
	matching := make([]*indexpb.SegmentIndex, 0, len(candidates))
	for _, candidate := range candidates {
		signature := fmt.Sprintf("%d/%d/%d/%d/%d", candidate.GetCollectionID(), candidate.GetPartitionID(), candidate.GetSegmentID(), candidate.GetIndexID(), candidate.GetIndexVersion())
		unique[signature] = struct{}{}
		matches := candidate.GetPartitionID() == objectPath.partitionID &&
			candidate.GetSegmentID() == objectPath.segmentID &&
			candidate.GetIndexVersion() == objectPath.indexVersion
		if objectPath.layout == "COLLECTION_ROOTED" {
			matches = matches && candidate.GetCollectionID() == objectPath.collectionID
		}
		if matches {
			matching = append(matching, candidate)
		}
	}
	if len(matching) > 0 {
		return matching[0], len(unique) > 1
	}
	return candidates[0], len(unique) > 1
}

func v3FieldIndexForSegment(metadata *v3ScanMetadata, segmentIndex *indexpb.SegmentIndex) *indexpb.IndexInfo {
	if segmentIndex == nil {
		return nil
	}
	return metadata.fieldIndexes[v3IndexKey{collectionID: segmentIndex.GetCollectionID(), indexID: segmentIndex.GetIndexID()}]
}

func matchesV3ScanFilters(objectPath v3ObjectPath, segmentIndex *indexpb.SegmentIndex, fieldIndex *indexpb.IndexInfo, p *ScanV3IndexParams) bool {
	if p.CollectionID != 0 {
		collectionID := objectPath.collectionID
		if segmentIndex != nil {
			collectionID = segmentIndex.GetCollectionID()
		}
		if collectionID != p.CollectionID {
			return false
		}
	}
	partitionID := objectPath.partitionID
	segmentID := objectPath.segmentID
	if segmentIndex != nil {
		partitionID = segmentIndex.GetPartitionID()
		segmentID = segmentIndex.GetSegmentID()
	}
	if p.PartitionID != 0 && partitionID != p.PartitionID {
		return false
	}
	if p.SegmentID != 0 && segmentID != p.SegmentID {
		return false
	}
	if p.IndexID != 0 && (segmentIndex == nil || segmentIndex.GetIndexID() != p.IndexID) {
		return false
	}
	if p.FieldID != 0 && (fieldIndex == nil || fieldIndex.GetFieldID() != p.FieldID) {
		return false
	}
	return true
}

func populateV3Metadata(record *V3IndexScanRecord, metadata *v3ScanMetadata, segmentIndex *indexpb.SegmentIndex, fieldIndex *indexpb.IndexInfo, filename, rootPath string) {
	record.MetadataCollectionID = segmentIndex.GetCollectionID()
	record.MetadataPartitionID = segmentIndex.GetPartitionID()
	record.MetadataSegmentID = segmentIndex.GetSegmentID()
	record.MetadataIndexID = segmentIndex.GetIndexID()
	record.MetadataIndexVersion = segmentIndex.GetIndexVersion()
	record.MetadataScalarIndexVersion = segmentIndex.GetCurrentScalarIndexVersion()
	record.IndexStorePathVersion = v3SegmentIndexPathVersion(segmentIndex)
	if fieldIndex == nil {
		record.MetadataIndexType = segmentIndex.GetIndexType()
		addV3Finding(record, "INDEX_METADATA_NOT_FOUND", v3SeverityCritical,
			fmt.Sprintf("collection index %d is missing for collection %d", segmentIndex.GetIndexID(), segmentIndex.GetCollectionID()))
	} else {
		record.MetadataFieldID = fieldIndex.GetFieldID()
		record.MetadataIndexType = v3IndexParam(fieldIndex, "index_type")
		if record.MetadataIndexType == "" {
			record.MetadataIndexType = segmentIndex.GetIndexType()
		}
		field := resolveV3EffectiveField(metadata.collections[segmentIndex.GetCollectionID()], fieldIndex)
		record.MetadataFieldName = field.name
		record.MetadataFieldType = field.rawType.String()
		record.JSONPath = field.jsonPath
		record.JSONCastType = field.jsonCastType
		record.IsJSONPathIndex = field.isJSONPathIndex
		record.isJSONPathIndex = field.isJSONPathIndex
		record.effectiveType = field.effectiveType
		record.effectiveTypeKnown = field.known
		if field.known {
			record.EffectiveFieldType = field.effectiveType.String()
		} else {
			record.EffectiveFieldType = "UNKNOWN"
			addV3Finding(record, "EFFECTIVE_TYPE_UNKNOWN", v3SeverityInfo,
				fmt.Sprintf("cannot resolve effective type for field %d", fieldIndex.GetFieldID()))
			if field.isJSONPathIndex {
				addV3Finding(record, "JSON_PATH_CAST_TYPE_UNKNOWN", v3SeverityError,
					fmt.Sprintf("cannot resolve JSON path %q cast type %q", field.jsonPath, field.jsonCastType))
			}
		}
		if field.isJSONPathIndex {
			record.ExpectedLoader = expectedV3JSONLoader(record.MetadataIndexType, field.effectiveType, field.known)
		} else {
			record.ExpectedLoader = expectedV3Loader(field.effectiveType, field.known)
		}
	}

	record.ResolvedPath = buildV3ResolvedPath(record.IndexStorePathVersion, segmentIndex, filename, rootPath)
	for _, key := range segmentIndex.GetIndexFileKeys() {
		if path.Base(key) != filename {
			continue
		}
		record.AdvertisedPath = normalizeV3AdvertisedPath(rootPath, key, record.ResolvedPath)
		break
	}
	if record.AdvertisedPath == "" {
		addV3Finding(record, "FILENAME_META_MISMATCH", v3SeverityError,
			fmt.Sprintf("SegmentIndex does not advertise filename %q", filename))
	} else if path.Clean(record.AdvertisedPath) != path.Clean(record.ResolvedPath) {
		addV3Finding(record, "ADVERTISED_RESOLVED_PATH_MISMATCH", v3SeverityCritical,
			fmt.Sprintf("advertised path %q differs from FileManager resolved path %q", record.AdvertisedPath, record.ResolvedPath))
	}
}

func checkV3PathMetadata(record *V3IndexScanRecord, objectPath v3ObjectPath, segmentIndex *indexpb.SegmentIndex) {
	mismatches := make([]string, 0)
	if objectPath.buildID != segmentIndex.GetBuildID() {
		mismatches = append(mismatches, fmt.Sprintf("build %d != %d", objectPath.buildID, segmentIndex.GetBuildID()))
	}
	if objectPath.indexVersion != segmentIndex.GetIndexVersion() {
		mismatches = append(mismatches, fmt.Sprintf("index version %d != %d", objectPath.indexVersion, segmentIndex.GetIndexVersion()))
	}
	if objectPath.partitionID != segmentIndex.GetPartitionID() {
		mismatches = append(mismatches, fmt.Sprintf("partition %d != %d", objectPath.partitionID, segmentIndex.GetPartitionID()))
	}
	if objectPath.segmentID != segmentIndex.GetSegmentID() {
		mismatches = append(mismatches, fmt.Sprintf("segment %d != %d", objectPath.segmentID, segmentIndex.GetSegmentID()))
	}
	if objectPath.layout == "COLLECTION_ROOTED" && objectPath.collectionID != segmentIndex.GetCollectionID() {
		mismatches = append(mismatches, fmt.Sprintf("collection %d != %d", objectPath.collectionID, segmentIndex.GetCollectionID()))
	}
	expectedLayout := "BUILD_ROOTED"
	if record.IndexStorePathVersion >= 1 {
		expectedLayout = "COLLECTION_ROOTED"
	}
	if objectPath.layout != "" && objectPath.layout != expectedLayout {
		mismatches = append(mismatches, fmt.Sprintf("layout %s != %s", objectPath.layout, expectedLayout))
	}
	if len(mismatches) > 0 {
		addV3Finding(record, "PATH_METADATA_MISMATCH", v3SeverityCritical, strings.Join(mismatches, "; "))
	}
	if record.ResolvedPath != "" && path.Clean(record.ResolvedPath) != path.Clean(record.ObjectKey) {
		addV3Finding(record, "ADVERTISED_RESOLVED_PATH_MISMATCH", v3SeverityCritical,
			fmt.Sprintf("scanned object %q differs from FileManager resolved object %q", record.ObjectKey, record.ResolvedPath))
	}
}

type v3EffectiveField struct {
	name            string
	rawType         schemapb.DataType
	effectiveType   schemapb.DataType
	jsonPath        string
	jsonCastType    string
	isJSONPathIndex bool
	known           bool
}

func resolveV3EffectiveField(collection *models.Collection, indexInfo *indexpb.IndexInfo) v3EffectiveField {
	result := v3EffectiveField{}
	if collection == nil || collection.GetProto().GetSchema() == nil || indexInfo == nil {
		return result
	}
	fieldID := indexInfo.GetFieldID()
	var field *schemapb.FieldSchema
	for _, candidate := range collection.GetProto().GetSchema().GetFields() {
		if candidate.GetFieldID() == fieldID {
			field = candidate
			result.name = candidate.GetName()
			break
		}
	}
	if field == nil {
		for _, structField := range collection.GetProto().GetSchema().GetStructArrayFields() {
			for _, candidate := range structField.GetFields() {
				if candidate.GetFieldID() == fieldID {
					field = candidate
					result.name = fmt.Sprintf("%s[%s]", structField.GetName(), candidate.GetName())
					break
				}
			}
			if field != nil {
				break
			}
		}
	}
	if field == nil {
		return result
	}
	result.rawType = field.GetDataType()
	result.effectiveType = field.GetDataType()
	result.known = true
	if field.GetDataType() == schemapb.DataType_Array {
		result.effectiveType = field.GetElementType()
		result.known = result.effectiveType != schemapb.DataType_None
	}
	if field.GetDataType() == schemapb.DataType_JSON {
		result.isJSONPathIndex = true
		result.jsonPath = v3IndexParam(indexInfo, "json_path")
		result.jsonCastType = strings.ToUpper(strings.TrimSpace(v3IndexParam(indexInfo, "json_cast_type")))
		result.effectiveType, result.known = parseV3CastType(result.jsonCastType)
	}
	return result
}

func parseV3CastType(value string) (schemapb.DataType, bool) {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	normalized = strings.TrimPrefix(normalized, "DATATYPE_")
	types := map[string]schemapb.DataType{
		"BOOL":          schemapb.DataType_Bool,
		"DOUBLE":        schemapb.DataType_Double,
		"VARCHAR":       schemapb.DataType_VarChar,
		"ARRAY_BOOL":    schemapb.DataType_Bool,
		"ARRAY_DOUBLE":  schemapb.DataType_Double,
		"ARRAY_VARCHAR": schemapb.DataType_VarChar,
		"JSON":          schemapb.DataType_JSON,
	}
	result, ok := types[normalized]
	return result, ok
}

func v3IndexParam(indexInfo *indexpb.IndexInfo, key string) string {
	if indexInfo == nil {
		return ""
	}
	var result string
	groups := [][]*commonpb.KeyValuePair{
		indexInfo.GetTypeParams(),
		indexInfo.GetUserIndexParams(),
		indexInfo.GetIndexParams(),
	}
	for _, group := range groups {
		for _, pair := range group {
			if strings.EqualFold(pair.GetKey(), key) {
				result = pair.GetValue()
			}
		}
	}
	return result
}

func checkV3MetaCompatibility(record *V3IndexScanRecord, segmentIndex *indexpb.SegmentIndex, segment *models.Segment) {
	if record.Meta == nil {
		return
	}
	internalType, hasInternalType := v3MetaInt64(record.Meta, "index_type")
	_, hasVersion := record.Meta["version"]
	_, hasIndexLength := record.Meta["index_length"]
	standaloneSTLSORT := isV3STLSORTIndexType(record.MetadataIndexType)
	if (hasInternalType && internalType == 2) || standaloneSTLSORT {
		switch {
		case hasVersion && hasIndexLength:
			addV3Finding(record, "STLSORT_META_CONFLICT", v3SeverityError,
				"STLSORT __meta__ contains both version and index_length")
		case !hasVersion && !hasIndexLength:
			addV3Finding(record, "STLSORT_META_AMBIGUOUS", v3SeverityError,
				"STLSORT __meta__ contains neither version nor index_length")
		}
	}
	if record.effectiveTypeKnown {
		numericSTLSORT := record.PhysicalIndexType == "HYBRID_STLSORT_NUMERIC" ||
			(standaloneSTLSORT && record.PhysicalIndexType == "STLSORT_NUMERIC")
		stringSTLSORT := record.PhysicalIndexType == "HYBRID_STLSORT_STRING" ||
			(standaloneSTLSORT && record.PhysicalIndexType == "STLSORT_STRING")
		if isV3StringType(record.effectiveType) && numericSTLSORT {
			if record.isJSONPathIndex {
				addV3Finding(record, "JSON_PATH_VARCHAR_NUMERIC_STLSORT", v3SeverityCritical,
					fmt.Sprintf("JSON path %q with json_cast_type=%s is associated with numeric STLSORT meta; StringIndexSort will request missing key 'version'", record.JSONPath, record.JSONCastType))
			} else {
				addV3Finding(record, "STRING_FIELD_NUMERIC_STLSORT", v3SeverityCritical,
					fmt.Sprintf("%s field is associated with numeric STLSORT meta; StringIndexSort will request missing key 'version'", record.EffectiveFieldType))
			}
		}
		if isV3NumericType(record.effectiveType) && stringSTLSORT {
			if record.isJSONPathIndex {
				addV3Finding(record, "JSON_PATH_NUMERIC_STRING_STLSORT", v3SeverityCritical,
					fmt.Sprintf("JSON path %q with json_cast_type=%s is associated with StringIndexSort meta", record.JSONPath, record.JSONCastType))
			} else {
				addV3Finding(record, "NUMERIC_FIELD_STRING_STLSORT", v3SeverityCritical,
					fmt.Sprintf("%s field is associated with StringIndexSort meta", record.EffectiveFieldType))
			}
		}
	}
	metaRows, hasMetaRows := v3MetaInt64(record.Meta, "num_rows")
	if !hasMetaRows {
		metaRows, hasMetaRows = v3MetaInt64(record.Meta, "bitmap_index_num_rows")
	}
	if hasMetaRows {
		expectedRows := make([]string, 0, 2)
		if segmentIndex != nil && segmentIndex.GetNumRows() > 0 && segmentIndex.GetNumRows() != metaRows {
			expectedRows = append(expectedRows, fmt.Sprintf("SegmentIndex=%d", segmentIndex.GetNumRows()))
		}
		if segment != nil && segment.GetNumOfRows() > 0 && segment.GetNumOfRows() != metaRows {
			expectedRows = append(expectedRows, fmt.Sprintf("Segment=%d", segment.GetNumOfRows()))
		}
		if len(expectedRows) > 0 {
			addV3Finding(record, "ROW_COUNT_MISMATCH", v3SeverityError,
				fmt.Sprintf("V3 meta rows=%d, %s", metaRows, strings.Join(expectedRows, ", ")))
		}
	}
}

func checkV3Filename(record *V3IndexScanRecord, filename string) {
	const prefix = "milvus_packed_"
	const suffix = "_index.v3"
	lower := strings.ToLower(filename)
	if !strings.HasPrefix(lower, prefix) || !strings.HasSuffix(lower, suffix) {
		return
	}
	fileType := strings.TrimSuffix(strings.TrimPrefix(lower, prefix), suffix)
	metadataType := strings.ToLower(record.MetadataIndexType)
	if metadataType != "" && metadataType != fileType {
		addV3Finding(record, "FILENAME_META_MISMATCH", v3SeverityError,
			fmt.Sprintf("filename declares %s but collection index type is %s", strings.ToUpper(fileType), record.MetadataIndexType))
	}
	if fileType == "hybrid" {
		if _, ok := v3MetaInt64(record.Meta, "index_type"); !ok {
			addV3Finding(record, "FILENAME_META_MISMATCH", v3SeverityError,
				"HYBRID filename has no internal index_type in __meta__")
		}
	}
}

func buildV3ResolvedPath(pathVersion int32, segmentIndex *indexpb.SegmentIndex, filename, rootPath string) string {
	if segmentIndex == nil {
		return ""
	}
	if pathVersion >= 1 {
		return path.Join(rootPath, "index_v1",
			strconv.FormatInt(segmentIndex.GetCollectionID(), 10),
			strconv.FormatInt(segmentIndex.GetPartitionID(), 10),
			strconv.FormatInt(segmentIndex.GetSegmentID(), 10),
			strconv.FormatInt(segmentIndex.GetBuildID(), 10),
			strconv.FormatInt(segmentIndex.GetIndexVersion(), 10),
			filename)
	}
	return path.Join(rootPath, "index_files",
		strconv.FormatInt(segmentIndex.GetBuildID(), 10),
		strconv.FormatInt(segmentIndex.GetIndexVersion(), 10),
		strconv.FormatInt(segmentIndex.GetPartitionID(), 10),
		strconv.FormatInt(segmentIndex.GetSegmentID(), 10),
		filename)
}

func normalizeV3AdvertisedPath(rootPath, advertised, resolved string) string {
	cleaned := oss.ResolveObjectKey(rootPath, advertised)
	if cleaned == "" || !strings.Contains(cleaned, "/") {
		return resolved
	}
	if strings.HasPrefix(cleaned, "index_files/") || strings.HasPrefix(cleaned, "index_v1/") {
		return path.Join(rootPath, cleaned)
	}
	return cleaned
}

func v3SegmentIndexPathVersion(segmentIndex *indexpb.SegmentIndex) int32 {
	if segmentIndex == nil {
		return 0
	}
	message := segmentIndex.ProtoReflect()
	if field := message.Descriptor().Fields().ByNumber(protoreflect.FieldNumber(22)); field != nil {
		return int32(message.Get(field).Enum())
	}
	unknown := message.GetUnknown()
	for len(unknown) > 0 {
		number, wireType, tagLength := protowire.ConsumeTag(unknown)
		if tagLength < 0 {
			break
		}
		unknown = unknown[tagLength:]
		if number == 22 && wireType == protowire.VarintType {
			value, valueLength := protowire.ConsumeVarint(unknown)
			if valueLength < 0 {
				return 0
			}
			return int32(value)
		}
		fieldLength := protowire.ConsumeFieldValue(number, wireType, unknown)
		if fieldLength < 0 {
			break
		}
		unknown = unknown[fieldLength:]
	}
	return 0
}

func objectChangedSinceList(object oss.ObjectInfo, stat *models.FsStat) bool {
	if stat == nil {
		return false
	}
	if object.Size != stat.Size {
		return true
	}
	if object.ETag != "" && stat.ETag != "" && object.ETag != stat.ETag {
		return true
	}
	if object.VersionID != "" && stat.VersionID != "" && object.VersionID != stat.VersionID {
		return true
	}
	return !object.LastModified.IsZero() && !stat.LastModified.IsZero() && !object.LastModified.Equal(stat.LastModified)
}

func addV3Finding(record *V3IndexScanRecord, rule, severity, message string) {
	for i := range record.Findings {
		if record.Findings[i].Rule == rule {
			if record.Findings[i].Message != message {
				record.Findings[i].Message += "; " + message
			}
			if v3SeverityRank(severity) < v3SeverityRank(record.Findings[i].Severity) {
				record.Findings[i].Severity = severity
			}
			return
		}
	}
	record.Findings = append(record.Findings, V3IndexFinding{Rule: rule, Severity: severity, Message: message})
}

func v3SeverityRank(severity string) int {
	switch severity {
	case v3SeverityCritical:
		return 0
	case v3SeverityError:
		return 1
	case v3SeverityWarning:
		return 2
	default:
		return 3
	}
}
