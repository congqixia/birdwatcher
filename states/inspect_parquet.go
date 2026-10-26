package states

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/ossutil"
	binlogv1 "github.com/milvus-io/birdwatcher/storage/binlog/v1"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
)

type InspectParquetParam struct {
	OSSOverrideParam
	framework.ParamBase `use:"inspect-parquet" desc:"inspect parquet file metadata and optionally sample rows"`
	FilePath            string `name:"file" default:"" desc:"local parquet file path to inspect"`
	SegmentID           int64  `name:"segment" default:"0" desc:"segment ID to inspect binlogs from remote storage"`
	FieldID             int64  `name:"field" default:"0" desc:"only inspect binlogs of this field ID (0 means all fields)"`
	StorageVersion      string `name:"storageVersion" default:"meta" desc:"segment storage version strategy: meta, auto, or a non-negative version override"`
	MetadataOnly        bool   `name:"metadataOnly" default:"true" desc:"print metadata only; set to false to also sample rows"`
	SampleRows          int64  `name:"sampleRows" default:"10" desc:"number of rows to sample when metadataOnly=false"`
	ShowRowGroups       bool   `name:"showRowGroups" default:"false" desc:"print per-row-group statistics"`
	MinioAddress        string `name:"minioAddr" default:"" desc:"override minio address"`
	SkipBucketCheck     bool   `name:"skipBucketCheck" default:"false" desc:"skip bucket existence check"`
	External            bool   `name:"external" default:"false" desc:"inspect parquet from external collection storage"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection ID for external collection mode"`
	ManifestSegmentID   int64  `name:"manifestSegment" default:"0" desc:"segment ID whose manifest files should be inspected in external mode"`
	ExternalFile        string `name:"externalFile" default:"" desc:"single parquet object path to inspect in external mode"`
}

const inspectAutoStorageVersion int64 = -1 << 63

type inspectStorageVersionMode int

const (
	inspectStorageVersionFromMeta inspectStorageVersionMode = iota
	inspectStorageVersionAuto
	inspectStorageVersionOverride
)

type inspectStorageVersionOption struct {
	mode    inspectStorageVersionMode
	version int64
}

func parseInspectStorageVersionOption(raw string) (inspectStorageVersionOption, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	switch value {
	case "", "meta":
		return inspectStorageVersionOption{mode: inspectStorageVersionFromMeta}, nil
	case "auto":
		return inspectStorageVersionOption{mode: inspectStorageVersionAuto}, nil
	}

	version, err := strconv.ParseInt(value, 10, 64)
	if err != nil || version < 0 {
		return inspectStorageVersionOption{}, errors.Newf(
			"invalid --storageVersion %q: expected meta, auto, or a non-negative integer", raw)
	}
	return inspectStorageVersionOption{mode: inspectStorageVersionOverride, version: version}, nil
}

// externalSourceSpec is the parsed external collection specification.
// It is an alias of ossutil.ExternalSourceSpec; kept for compatibility.
type externalSourceSpec = ossutil.ExternalSourceSpec

// externalSourceLocation is the parsed external_source URI.
type externalSourceLocation = ossutil.ExternalSourceLocation

func (s *InstanceState) InspectParquetCommand(ctx context.Context, p *InspectParquetParam) error {
	if err := validateInspectParquetParam(p); err != nil {
		return err
	}

	if p.External {
		return s.inspectExternalCollectionParquet(ctx, p)
	}
	if p.FilePath != "" {
		return s.inspectLocalParquet(ctx, p)
	}
	return s.inspectSegmentParquet(ctx, p)
}

func validateInspectParquetParam(p *InspectParquetParam) error {
	storageVersion, err := parseInspectStorageVersionOption(p.StorageVersion)
	if err != nil {
		return err
	}
	if storageVersion.mode != inspectStorageVersionFromMeta && (p.External || p.FilePath != "") {
		return errors.New("--storageVersion can only be used with --segment")
	}

	if p.External {
		if p.FilePath != "" || p.SegmentID != 0 {
			return errors.New("--external cannot be used with --file or --segment")
		}
		if p.CollectionID == 0 {
			return errors.New("--collection must be provided in external mode")
		}
		if p.ManifestSegmentID == 0 && p.ExternalFile == "" {
			return errors.New("either --manifestSegment or --externalFile must be provided in external mode")
		}
		if p.ManifestSegmentID != 0 && p.ExternalFile != "" {
			return errors.New("--manifestSegment and --externalFile are mutually exclusive in external mode")
		}
		return nil
	}

	if p.FilePath == "" && p.SegmentID == 0 {
		return errors.New("either --file or --segment must be provided")
	}
	if p.FilePath != "" && p.SegmentID != 0 {
		return errors.New("--file and --segment are mutually exclusive")
	}
	return nil
}

func (s *InstanceState) inspectLocalParquet(ctx context.Context, p *InspectParquetParam) error {
	f, err := openBackupFile(p.FilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	pqReader, err := file.NewParquetReader(f)
	if err != nil {
		return errors.Wrapf(err, "failed to open parquet file %s", p.FilePath)
	}
	defer pqReader.Close()

	return printParquetFile(ctx, pqReader, p.FilePath, p.MetadataOnly, p.SampleRows, p.ShowRowGroups)
}

func (s *InstanceState) inspectSegmentParquet(ctx context.Context, p *InspectParquetParam) error {
	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(seg *models.Segment) bool {
		return seg.ID == p.SegmentID
	})
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return errors.Newf("segment %d not found", p.SegmentID)
	}
	segment := segments[0]
	fmt.Printf("Segment %d: collection=%d partition=%d storageVersion=%d\n",
		segment.ID, segment.CollectionID, segment.PartitionID, segment.StorageVersion)

	storageVersionOption, err := parseInspectStorageVersionOption(p.StorageVersion)
	if err != nil {
		return err
	}
	effectiveStorageVersion := segment.GetStorageVersion()
	switch storageVersionOption.mode {
	case inspectStorageVersionFromMeta:
		fmt.Printf("Storage Version Strategy: meta (effective=%d)\n", effectiveStorageVersion)
	case inspectStorageVersionOverride:
		effectiveStorageVersion = storageVersionOption.version
		fmt.Printf("Storage Version Strategy: override (meta=%d effective=%d)\n",
			segment.GetStorageVersion(), effectiveStorageVersion)
	case inspectStorageVersionAuto:
		if segment.GetManifestPath() != "" {
			effectiveStorageVersion = 3
			fmt.Printf("Storage Version Strategy: auto (manifest layout detected)\n")
		} else {
			effectiveStorageVersion = inspectAutoStorageVersion
			fmt.Printf("Storage Version Strategy: auto (detect each binlog object)\n")
		}
	}

	resolvedStore, err := s.getObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, p.SkipBucketCheck)
	if err != nil {
		return err
	}
	rootPath := resolvedStore.RootPath

	if effectiveStorageVersion >= 3 {
		if segment.GetManifestPath() == "" {
			return errors.Newf("segment %d effective storage version is %d but got empty manifest", segment.GetID(), effectiveStorageVersion)
		}
		return inspectV3SegmentParquet(ctx, resolvedStore.Store, rootPath, segment, p)
	}

	for _, fieldBinlog := range segment.GetBinlogs() {
		if p.FieldID != 0 && fieldBinlog.FieldID != p.FieldID {
			continue
		}
		for _, binlog := range fieldBinlog.Binlogs {
			logPath := oss.ResolveObjectKey(rootPath, binlog.LogPath)
			fmt.Printf("\n===== Field %d | %s =====\n", fieldBinlog.FieldID, logPath)
			if err := inspectRemoteBinlog(ctx, resolvedStore.Store, logPath, effectiveStorageVersion, p.MetadataOnly, p.SampleRows, p.ShowRowGroups); err != nil {
				fmt.Printf("failed to inspect %s: %s\n", logPath, err.Error())
			}
		}
	}
	return nil
}

func (s *InstanceState) inspectExternalCollectionParquet(ctx context.Context, p *InspectParquetParam) error {
	collection, err := common.GetCollectionByIDVersion(ctx, s.client, s.basePath, p.CollectionID)
	if err != nil {
		return err
	}
	proto := collection.GetProto()
	if proto.GetSchema().GetExternalSource() == "" {
		return errors.Newf("collection %d does not have external source", p.CollectionID)
	}

	spec, err := parseExternalSpec(proto.GetSchema().GetExternalSpec())
	if err != nil {
		return err
	}
	externalMinioClient, externalBucketName, externalRootPath, location, err := newExternalMinioClient(ctx, proto.GetSchema().GetExternalSource(), spec, p.SkipBucketCheck)
	if err != nil {
		return err
	}
	externalStore := oss.NewMinioObjectStoreWithBucket(externalMinioClient, externalBucketName)

	fmt.Printf("External Collection %d: name=%s\n", proto.GetID(), proto.GetSchema().GetName())
	fmt.Printf("External Source: %s\n", proto.GetSchema().GetExternalSource())
	fmt.Printf("Resolved Provider=%s Region=%s Host=%s Bucket=%s RootPath=%s\n", spec.CloudProvider, spec.Region, location.Host, externalBucketName, externalRootPath)

	if p.ExternalFile != "" {
		objectKey, err := resolveExternalObjectKey(location, p.ExternalFile)
		if err != nil {
			return err
		}
		fmt.Printf("Mode: external-file\n")
		fmt.Printf("Object: %s\n", objectKey)
		return inspectRemoteParquetObject(ctx, externalStore, objectKey, p.MetadataOnly, p.SampleRows, p.ShowRowGroups)
	}

	manifestStore, err := s.getObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, p.SkipBucketCheck)
	if err != nil {
		return err
	}

	fmt.Printf("Mode: manifest-segment\n")
	fmt.Printf("Manifest Storage: bucket=%s rootPath=%s\n", manifestStore.BucketName, manifestStore.RootPath)
	return s.inspectExternalManifestSegmentParquet(ctx, manifestStore.Store, manifestStore.RootPath, externalStore, location, collection, p)
}

func (s *InstanceState) inspectExternalManifestSegmentParquet(ctx context.Context, manifestStore oss.ObjectStore, manifestRootPath string, externalStore oss.ObjectStore, location externalSourceLocation, collection *models.Collection, p *InspectParquetParam) error {
	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(seg *models.Segment) bool {
		return seg.ID == p.ManifestSegmentID
	})
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return errors.Newf("segment %d not found", p.ManifestSegmentID)
	}
	segment := segments[0]
	if segment.GetCollectionID() != collection.GetProto().GetID() {
		return errors.Newf("segment %d belongs to collection %d, not external collection %d", p.ManifestSegmentID, segment.GetCollectionID(), collection.GetProto().GetID())
	}
	if segment.GetManifestPath() == "" {
		return errors.Newf("segment %d does not have manifest path", segment.GetID())
	}

	fmt.Printf("Manifest Segment: %d\n", segment.GetID())
	return inspectExternalManifestParquet(ctx, manifestStore, manifestRootPath, externalStore, location, segment, p)
}

func inspectExternalManifestParquet(ctx context.Context, manifestStore oss.ObjectStore, manifestRootPath string, externalStore oss.ObjectStore, location externalSourceLocation, segment *models.Segment, p *InspectParquetParam) error {
	rawManifest := segment.GetManifestPath()
	fmt.Printf("Manifest Raw: %s\n", rawManifest)

	var manifestRef struct {
		Ver      int    `json:"ver"`
		BasePath string `json:"base_path"`
	}
	if err := json.Unmarshal([]byte(rawManifest), &manifestRef); err != nil {
		return errors.Wrap(err, "parse manifest path JSON")
	}

	manifestBasePath := oss.ResolveObjectKey(manifestRootPath, manifestRef.BasePath)
	manifestPath := path.Join(manifestBasePath, "_metadata", fmt.Sprintf("manifest-%d.avro", manifestRef.Ver))
	fmt.Printf("Manifest File: %s\n", manifestPath)

	obj, err := manifestStore.Open(ctx, manifestPath)
	if err != nil {
		return errors.Wrap(err, "get manifest object")
	}
	m, err := parseManifest(obj)
	if err != nil {
		return errors.Wrap(err, "parse manifest")
	}

	fieldFilter := ""
	if p.FieldID != 0 {
		fieldFilter = strconv.FormatInt(p.FieldID, 10)
	}

	resolver := ossutil.NewManifestPathResolver(manifestStore, manifestBasePath, externalStore, location)

	for i, cg := range m.ColumnGroups {
		if fieldFilter != "" && !slices.Contains(cg.Columns, fieldFilter) {
			continue
		}
		fmt.Printf("\n===== Column Group #%d | columns=%v format=%s =====\n", i, cg.Columns, cg.Format)
		for _, f := range cg.Files {
			store, filePath, backend, err := resolver.Resolve(f.Path, "_data")
			if err != nil {
				fmt.Printf("failed to resolve manifest file path %s: %s\n", f.Path, err.Error())
				continue
			}
			fmt.Printf("\n----- File %s (rows: [%d, %d)) backend=%s -----\n", filePath, f.StartIndex, f.EndIndex, backend)
			if err := inspectRemoteParquetObject(ctx, store, filePath, p.MetadataOnly, p.SampleRows, p.ShowRowGroups); err != nil {
				fmt.Printf("failed to inspect %s: %s\n", filePath, err.Error())
			}
		}
	}
	return nil
}

func inspectV3SegmentParquet(ctx context.Context, store oss.ObjectStore, rootPath string, segment *models.Segment, p *InspectParquetParam) error {
	rawManifest := segment.GetManifestPath()
	fmt.Printf("Manifest Raw: %s\n", rawManifest)

	var manifestRef struct {
		Ver      int    `json:"ver"`
		BasePath string `json:"base_path"`
	}
	if err := json.Unmarshal([]byte(rawManifest), &manifestRef); err != nil {
		return errors.Wrap(err, "parse manifest path JSON")
	}

	basePath := oss.ResolveObjectKey(rootPath, manifestRef.BasePath)
	manifestPath := path.Join(basePath, "_metadata", fmt.Sprintf("manifest-%d.avro", manifestRef.Ver))
	fmt.Printf("Manifest File: %s\n", manifestPath)

	obj, err := store.Open(ctx, manifestPath)
	if err != nil {
		return errors.Wrap(err, "get manifest object")
	}
	m, err := parseManifest(obj)
	if err != nil {
		return errors.Wrap(err, "parse manifest")
	}

	fieldFilter := ""
	if p.FieldID != 0 {
		fieldFilter = strconv.FormatInt(p.FieldID, 10)
	}

	for i, cg := range m.ColumnGroups {
		if fieldFilter != "" && !slices.Contains(cg.Columns, fieldFilter) {
			continue
		}
		fmt.Printf("\n===== Column Group #%d | columns=%v format=%s =====\n", i, cg.Columns, cg.Format)
		for _, f := range cg.Files {
			filePath := path.Join(basePath, "_data", oss.ResolveObjectKey(rootPath, f.Path))
			fmt.Printf("\n----- File %s (rows: [%d, %d)) -----\n", filePath, f.StartIndex, f.EndIndex)
			if err := inspectRemoteBinlog(ctx, store, filePath, 2, p.MetadataOnly, p.SampleRows, p.ShowRowGroups); err != nil {
				fmt.Printf("failed to inspect %s: %s\n", filePath, err.Error())
			}
		}
	}
	return nil
}

func inspectRemoteParquetObject(ctx context.Context, store oss.ObjectStore, objectKey string, metadataOnly bool, sampleRows int64, showRowGroups bool) error {
	obj, err := store.Open(ctx, objectKey)
	if err != nil {
		return err
	}
	if closer, ok := obj.(interface{ Close() error }); ok {
		defer closer.Close()
	}

	pqReader, err := file.NewParquetReader(obj)
	if err != nil {
		return err
	}
	defer pqReader.Close()

	return printParquetFile(ctx, pqReader, path.Base(objectKey), metadataOnly, sampleRows, showRowGroups)
}

func inspectRemoteBinlog(ctx context.Context, store oss.ObjectStore, logPath string, storageVersion int64, metadataOnly bool, sampleRows int64, showRowGroups bool) error {
	obj, err := store.Open(ctx, logPath)
	if err != nil {
		return err
	}
	if closer, ok := obj.(interface{ Close() error }); ok {
		defer closer.Close()
	}

	pqReader, detectedStorageVersion, err := openBinlogParquet(obj, storageVersion)
	if err != nil {
		return err
	}
	defer pqReader.Close()
	if storageVersion == inspectAutoStorageVersion {
		fmt.Printf("Detected Storage Version: %d (%s)\n", detectedStorageVersion, path.Base(logPath))
	}

	return printParquetFile(ctx, pqReader, path.Base(logPath), metadataOnly, sampleRows, showRowGroups)
}

func openBinlogParquet(r storagecommon.ReadSeeker, storageVersion int64) (*file.Reader, int64, error) {
	switch storageVersion {
	case inspectAutoStorageVersion:
		detectedStorageVersion, err := detectBinlogStorageVersion(r)
		if err != nil {
			return nil, 0, err
		}
		return openBinlogParquet(r, detectedStorageVersion)
	case 2:
		pqReader, err := file.NewParquetReader(r)
		return pqReader, storageVersion, err
	case 0, 1:
		br, err := binlogv1.NewBinlogReader(r)
		if err != nil {
			return nil, 0, err
		}
		pqReader, err := br.NextParquetReader()
		return pqReader, storageVersion, err
	default:
		return nil, 0, errors.Newf("unsupported storage version: %d", storageVersion)
	}
}

func detectBinlogStorageVersion(r storagecommon.ReadSeeker) (int64, error) {
	var header [4]byte
	n, err := r.ReadAt(header[:], 0)
	if err != nil {
		return 0, errors.Wrapf(err, "read binlog header: got %d of %d bytes", n, len(header))
	}

	if string(header[:]) == "PAR1" {
		return 2, nil
	}
	if binary.LittleEndian.Uint32(header[:]) == uint32(binlogv1.MagicNumberV1) {
		return 0, nil
	}
	return 0, errors.Newf("unrecognized binlog header: %x", header)
}

func printParquetFile(ctx context.Context, pqReader *file.Reader, name string, metadataOnly bool, sampleRows int64, showRowGroups bool) error {
	printParquetMetadata(pqReader, name, showRowGroups)
	if metadataOnly {
		return nil
	}
	return samplePqRows(ctx, pqReader, sampleRows)
}

func printParquetMetadata(pqReader *file.Reader, name string, showRowGroups bool) {
	md := pqReader.MetaData()
	fmt.Printf("--- Parquet metadata: %s ---\n", name)
	fmt.Printf("NumRows: %d | NumRowGroups: %d\n", pqReader.NumRows(), pqReader.NumRowGroups())
	fmt.Printf("CreatedBy: %s | Version: %d\n", md.GetCreatedBy(), md.Version())

	schema := md.Schema
	fmt.Printf("Parquet Columns (%d):\n", schema.NumColumns())
	for i := 0; i < schema.NumColumns(); i++ {
		col := schema.Column(i)
		fmt.Printf("  [%d] %s (physical=%s logical=%s)\n",
			i, col.Name(), col.PhysicalType().String(), col.LogicalType().String())
	}

	if arrReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator); err == nil {
		if arrowSchema, err := arrReader.Schema(); err == nil {
			fmt.Printf("Arrow Schema:\n%s\n", arrowSchema.String())
		}
	}

	if showRowGroups {
		for rg := 0; rg < pqReader.NumRowGroups(); rg++ {
			rgMd := md.RowGroup(rg)
			fmt.Printf("RowGroup %d: NumRows=%d TotalByteSize=%d\n",
				rg, rgMd.NumRows(), rgMd.TotalByteSize())
		}
	}
}

func samplePqRows(ctx context.Context, pqReader *file.Reader, limit int64) error {
	if limit <= 0 {
		return nil
	}
	arrReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	rr, err := arrReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return err
	}
	defer rr.Release()

	fmt.Printf("--- Sample up to %d rows ---\n", limit)
	var printed int64
	for printed < limit && rr.Next() {
		rec := rr.Record()
		n := printRecordRows(rec, printed, limit-printed)
		printed += n
		if n == 0 {
			break
		}
	}
	if printed == 0 {
		fmt.Println("(no rows)")
	}
	return nil
}

func printRecordRows(rec arrow.Record, startIdx, limit int64) int64 {
	rows := min(rec.NumRows(), limit)
	cols := int(rec.NumCols())
	names := make([]string, cols)
	for i := range cols {
		names[i] = rec.ColumnName(i)
	}
	for i := range rows {
		parts := make([]string, cols)
		for c := range cols {
			parts[c] = fmt.Sprintf("%s=%s", names[c], arrowCellString(rec.Column(c), int(i)))
		}
		fmt.Printf("[%d] %s\n", startIdx+i, strings.Join(parts, ", "))
	}
	return rows
}

func arrowCellString(arr arrow.Array, idx int) string {
	if arr.IsNull(idx) {
		return "<null>"
	}
	type valueStr interface {
		ValueStr(int) string
	}
	if v, ok := arr.(valueStr); ok {
		return v.ValueStr(idx)
	}
	return arr.String()
}

func parseExternalSpec(raw string) (externalSourceSpec, error) {
	return ossutil.ParseExternalSpec(raw)
}

func parseExternalSource(raw string) (externalSourceLocation, error) {
	return ossutil.ParseExternalSource(raw)
}

func newExternalMinioClient(ctx context.Context, source string, spec externalSourceSpec, skipBucketCheck bool) (*minio.Client, string, string, externalSourceLocation, error) {
	store, location, err := ossutil.NewResolvedExternalObjectStore(ctx, source, spec, skipBucketCheck)
	if err != nil {
		return nil, "", "", externalSourceLocation{}, err
	}
	client, ok := oss.MinioClientFromObjectStore(store.Store)
	if !ok {
		return nil, "", "", externalSourceLocation{}, errors.New("external object store is not backed by minio client")
	}
	return client, store.BucketName, store.RootPath, location, nil
}

func inferCloudProviderFromScheme(scheme string) string {
	return ossutil.InferCloudProviderFromScheme(scheme)
}

func resolveExternalObjectKey(location externalSourceLocation, externalFile string) (string, error) {
	return ossutil.ResolveExternalObjectKey(location, externalFile)
}

func readMapString(payload map[string]any, key string) string {
	return ossutil.ReadMapString(payload, key)
}

func readMapBool(payload map[string]any, key string) (bool, bool) {
	return ossutil.ReadMapBool(payload, key)
}
