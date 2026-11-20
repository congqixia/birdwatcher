package states

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/ossutil"
	binlogv1 "github.com/milvus-io/birdwatcher/storage/binlog/v1"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type InspectParquetParam struct {
	oss.OSSOverrideParam
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
	ExternalFile        string `name:"externalFile" default:"" desc:"single parquet object path (Iceberg requires a complete URI)"`
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

type externalSourceSpec = ossutil.ExternalSourceSpec

type externalSourceLocation struct {
	Scheme       string
	SourceHost   string
	Host         string
	Bucket       string
	RootPath     string
	HostIsBucket bool
}

func toOSSUtilExternalSourceLocation(location externalSourceLocation) ossutil.ExternalSourceLocation {
	form := ossutil.LocationFormMilvus
	if location.HostIsBucket {
		form = ossutil.LocationFormAWS
	}
	return ossutil.ExternalSourceLocation{
		Scheme:   location.Scheme,
		Host:     location.SourceHost,
		Bucket:   location.Bucket,
		RootPath: location.RootPath,
		Address:  location.Host,
		Form:     form,
	}
}

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

	resolvedStore, err := oss.NewResolvedObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, p.SkipBucketCheck, s.GetObjectStore)
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

	spec, err := ossutil.ParseExternalSpecLoose(proto.GetSchema().GetExternalSpec())
	if err != nil {
		return err
	}
	if spec.Format != "" && spec.Format != externalspec.FormatParquet && spec.Format != externalspec.FormatIcebergTable {
		return errors.Newf("inspect-parquet does not support external format %s", spec.Format)
	}
	externalStore, externalBucketName, externalRootPath, location, err := newExternalObjectStore(ctx, proto.GetSchema().GetExternalSource(), spec, p.SkipBucketCheck)
	if err != nil {
		return err
	}

	fmt.Printf("External Collection %d: name=%s\n", proto.GetID(), proto.GetSchema().GetName())
	fmt.Printf("External Source: %s\n", redactExternalVectorSource(proto.GetSchema().GetExternalSource()))
	fmt.Printf("Resolved Provider=%s Region=%s Host=%s Bucket=%s RootPath=%s\n", spec.CloudProvider, spec.Region, location.Host, externalBucketName, externalRootPath)

	if spec.Format == externalspec.FormatIcebergTable {
		fmt.Println("Iceberg inspection reads physical Parquet files; deletes and fragment row ranges are not applied.")
	}
	if p.ExternalFile != "" {
		var objectKey string
		if spec.Format == externalspec.FormatIcebergTable {
			objectKey, err = ossutil.ResolveExternalObjectURI(toOSSUtilExternalSourceLocation(location), p.ExternalFile)
		} else {
			objectKey, err = resolveExternalObjectKey(location, p.ExternalFile)
		}
		if err != nil {
			return err
		}
		fmt.Printf("Mode: external-file\n")
		fmt.Printf("Object: %s\n", objectKey)
		return inspectRemoteParquetObject(ctx, externalStore, objectKey, p.MetadataOnly, p.SampleRows, p.ShowRowGroups)
	}

	manifestStore, err := oss.NewResolvedObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, p.SkipBucketCheck, s.GetObjectStore)
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
	return inspectExternalManifestParquet(ctx, manifestStore, manifestRootPath, externalStore, location, collection.GetProto().GetSchema(), segment, p)
}

func inspectExternalManifestParquet(ctx context.Context, manifestStore oss.ObjectStore, manifestRootPath string, externalStore oss.ObjectStore, location externalSourceLocation, schema *schemapb.CollectionSchema, segment *models.Segment, p *InspectParquetParam) error {
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
	if closer, ok := obj.(interface{ Close() error }); ok {
		defer closer.Close()
	}
	m, err := parseManifest(obj)
	if err != nil {
		return errors.Wrap(err, "parse manifest")
	}

	fieldColumn := ""
	if p.FieldID != 0 {
		helper, err := typeutil.CreateSchemaHelper(schema)
		if err != nil {
			return err
		}
		field, err := helper.GetFieldFromID(p.FieldID)
		if err != nil {
			return err
		}
		var stored bool
		fieldColumn, stored = typeutil.NewStorageColumnResolver(schema).ManifestStoredColumnName(field)
		if !stored {
			return errors.Newf("field %d is not stored in the manifest", p.FieldID)
		}
	}

	resolver := ossutil.NewManifestPathResolver(
		manifestStore,
		manifestBasePath,
		externalStore,
		toOSSUtilExternalSourceLocation(location),
	)

	pathResolutionFailures := 0
	inspectionFailures := 0
	matchedGroups := 0
	for i, cg := range m.ColumnGroups {
		if p.FieldID != 0 && !slices.Contains(cg.Columns, fieldColumn) {
			continue
		}
		matchedGroups++
		fmt.Printf("\n===== Column Group #%d | columns=%v format=%s =====\n", i, cg.Columns, cg.Format)
		for _, f := range cg.Files {
			var store oss.ObjectStore
			var filePath string
			var backend ossutil.FileBackend
			var err error
			if cg.Format == externalspec.FormatIcebergTable {
				filePath, err = ossutil.ResolveExternalObjectURI(toOSSUtilExternalSourceLocation(location), f.Path)
				store, backend = externalStore, ossutil.FileBackendExternal
			} else {
				store, filePath, backend, err = resolver.Resolve(f.Path, "_data")
			}
			if err != nil {
				fmt.Printf("failed to resolve manifest file path %s: %s\n",
					redactExternalVectorObjectKey(f.Path),
					sanitizeExternalVectorInspectionError(err, f.Path),
				)
				pathResolutionFailures++
				continue
			}
			fmt.Printf("\n----- File %s (fragment rows: [%d, %d)) backend=%s -----\n", filePath, f.StartIndex, f.EndIndex, backend)
			if err := inspectRemoteParquetObject(ctx, store, filePath, p.MetadataOnly, p.SampleRows, p.ShowRowGroups); err != nil {
				fmt.Printf("failed to inspect %s: %s\n", redactExternalVectorObjectKey(filePath), sanitizeExternalVectorInspectionError(err, filePath))
				inspectionFailures++
			}
		}
	}
	if p.FieldID != 0 && matchedGroups == 0 {
		return errors.Newf("field %d has no matching column group in the manifest", p.FieldID)
	}
	if pathResolutionFailures != 0 || inspectionFailures != 0 {
		return errors.Newf("manifest file failures: %d path resolution, %d inspection", pathResolutionFailures, inspectionFailures)
	}
	return nil
}

func resolveExternalManifestObjectKey(location externalSourceLocation, manifestBasePath, rawPath string) (string, error) {
	trimmed := strings.TrimSpace(rawPath)
	if trimmed == "" {
		return "", errors.New("manifest file path is empty")
	}
	if strings.Contains(trimmed, "://") {
		return resolveExternalObjectKey(location, trimmed)
	}
	if strings.Contains(trimmed, "ROOT_PATH") {
		replaced := strings.ReplaceAll(trimmed, "ROOT_PATH", location.RootPath)
		return path.Clean(strings.TrimPrefix(replaced, "/")), nil
	}
	trimmed = strings.TrimPrefix(trimmed, "/")
	if location.RootPath == "" {
		return path.Clean(trimmed), nil
	}
	if trimmed == location.RootPath || strings.HasPrefix(trimmed, location.RootPath+"/") {
		return path.Clean(trimmed), nil
	}
	if manifestBasePath != "" {
		basePath := strings.TrimPrefix(strings.TrimSpace(manifestBasePath), "/")
		if basePath != "" {
			dataPath := path.Join(basePath, "_data", trimmed)
			if strings.HasPrefix(dataPath, location.RootPath+"/") {
				return path.Clean(dataPath), nil
			}
		}
	}
	return path.Join(location.RootPath, trimmed), nil
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
	fmt.Printf("Physical NumRows: %d | NumRowGroups: %d\n", pqReader.NumRows(), pqReader.NumRowGroups())
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
	spec, err := ossutil.ParseExternalSpecLoose(raw)
	if err != nil {
		return externalSourceSpec{}, err
	}
	if spec.Format != "" && spec.Format != externalspec.FormatParquet &&
		spec.Format != externalspec.FormatLanceTable {
		return externalSourceSpec{}, errors.Newf("external collection format %s is not supported", spec.Format)
	}
	return spec, nil
}

func parseExternalSource(raw string, spec externalSourceSpec) (externalSourceLocation, error) {
	if err := externalspec.ValidateExternalSource(raw); err != nil {
		return externalSourceLocation{}, errors.New(sanitizeExternalVectorInspectionError(err, raw))
	}
	resolved, err := ossutil.ResolveExternalSource(raw, spec)
	if err != nil {
		return externalSourceLocation{}, err
	}
	if spec.BucketName != "" {
		resolved.Bucket = spec.BucketName
	}
	return externalSourceLocation{
		Scheme:       resolved.Scheme,
		SourceHost:   resolved.Host,
		Host:         resolved.Address,
		Bucket:       resolved.Bucket,
		RootPath:     resolved.RootPath,
		HostIsBucket: resolved.Form == ossutil.LocationFormAWS,
	}, nil
}

func newExternalObjectStore(ctx context.Context, source string, spec externalSourceSpec, skipBucketCheck bool) (oss.ObjectStore, string, string, externalSourceLocation, error) {
	param, location, err := buildExternalMinioClientParam(source, spec, skipBucketCheck)
	if err != nil {
		return nil, "", "", externalSourceLocation{}, err
	}
	if param.CloudProvider == oss.CloudProviderAzure {
		store, err := oss.NewAzureObjectStore(ctx, param)
		if err != nil {
			return nil, "", "", externalSourceLocation{}, err
		}
		return store, param.BucketName, param.RootPath, location, nil
	}
	client, err := oss.NewMinioClient(ctx, param)
	if err != nil {
		return nil, "", "", externalSourceLocation{}, err
	}
	return oss.NewMinioObjectStore(client), client.BucketName, client.RootPath, location, nil
}

func externalSourceLocationForSpec(source string, spec externalSourceSpec) (externalSourceLocation, error) {
	if err := ossutil.ValidateExternalStorageSpec(source, spec); err != nil {
		if !ossutil.IsLegacyExternalSpec(spec) {
			return externalSourceLocation{}, errors.New(
				sanitizeExternalVectorInspectionError(err, source))
		}
		return parseLegacyExternalSource(source)
	}
	return parseExternalSource(source, spec)
}

func buildExternalMinioClientParam(source string, spec externalSourceSpec, skipBucketCheck bool) (oss.MinioClientParam, externalSourceLocation, error) {
	if err := ossutil.ValidateExternalStorageSpec(source, spec); err != nil {
		if !ossutil.IsLegacyExternalSpec(spec) {
			return oss.MinioClientParam{}, externalSourceLocation{}, errors.New(
				sanitizeExternalVectorInspectionError(err, source))
		}
		return buildLegacyExternalMinioClientParam(source, spec, skipBucketCheck)
	}
	location, err := parseExternalSource(source, spec)
	if err != nil {
		return oss.MinioClientParam{}, externalSourceLocation{}, err
	}
	provider := spec.CloudProvider
	if location.Scheme == externalspec.SchemeMinIO && provider == "" {
		provider = externalspec.CloudProviderMinIO
	}
	if spec.RoleARN != "" && provider != externalspec.CloudProviderAWS &&
		provider != externalspec.CloudProviderMinIO &&
		provider != externalspec.CloudProviderAliyun {
		return oss.MinioClientParam{}, externalSourceLocation{}, errors.Newf(
			"extfs.role_arn is not supported for cloud provider %s by Birdwatcher object storage client",
			provider,
		)
	}
	if spec.GCPTargetServiceAccount != "" {
		return oss.MinioClientParam{}, externalSourceLocation{}, errors.New(
			"extfs.gcp_target_service_account is not supported by Birdwatcher object storage client",
		)
	}
	if spec.SSLCACert != "" {
		return oss.MinioClientParam{}, externalSourceLocation{}, errors.New(
			"extfs.ssl_ca_cert is not supported by Birdwatcher object storage client",
		)
	}

	useSSL := location.Scheme != externalspec.SchemeMinIO
	if spec.UseSSL != nil {
		useSSL = *spec.UseSSL
	}
	param := oss.MinioClientParam{
		Addr:                         location.Host,
		AK:                           spec.AccessKeyID,
		SK:                           spec.AccessKeyValue,
		UseIAM:                       spec.UseIAM,
		Anonymous:                    spec.Anonymous,
		IAMEndpoint:                  spec.IAMEndpoint,
		UseSSL:                       useSSL,
		UseVirtualHost:               spec.UseVirtualHost,
		CloudProvider:                provider,
		Region:                       spec.Region,
		RoleARN:                      spec.RoleARN,
		RoleSessionName:              spec.RoleSessionName,
		ExternalID:                   spec.ExternalID,
		AliyunRoleAuthMode:           spec.AliyunRoleAuthMode,
		LoadFrequency:                spec.LoadFrequency,
		AzureClientID:                spec.AzureClientID,
		AzureTenantID:                spec.AzureTenantID,
		AzureCredentialEndpoint:      spec.AzureCredentialEndpoint,
		AzureRequestTimeoutMs:        3000,
		DisableAzureConnectionString: provider == externalspec.CloudProviderAzure,
		BucketName:                   location.Bucket,
		RootPath:                     location.RootPath,
	}
	if provider == oss.CloudProviderAliyun && param.RoleARN != "" && param.AliyunRoleAuthMode == "" {
		param.AliyunRoleAuthMode = "oidc"
	}
	oss.WithSkipCheckBucket(skipBucketCheck)(&param)
	return param, location, nil
}

func buildLegacyExternalMinioClientParam(source string, spec externalSourceSpec, skipBucketCheck bool) (oss.MinioClientParam, externalSourceLocation, error) {
	location, err := parseLegacyExternalSource(source)
	if err != nil {
		return oss.MinioClientParam{}, externalSourceLocation{}, err
	}
	provider := spec.CloudProvider
	if provider == "" {
		provider = inferLegacyCloudProviderFromScheme(location.Scheme)
	}
	if provider == "" {
		return oss.MinioClientParam{}, externalSourceLocation{}, errors.Newf(
			"unsupported external source scheme/provider: %s/%s",
			location.Scheme,
			spec.CloudProvider,
		)
	}

	useSSL := location.Scheme != externalspec.SchemeMinIO
	if spec.UseSSL != nil {
		useSSL = *spec.UseSSL
	}
	param := oss.MinioClientParam{
		Addr:               location.Host,
		AK:                 spec.AccessKeyID,
		SK:                 spec.AccessKeyValue,
		UseIAM:             spec.UseIAM,
		Anonymous:          spec.Anonymous,
		IAMEndpoint:        spec.IAMEndpoint,
		UseSSL:             useSSL,
		UseVirtualHost:     spec.UseVirtualHost,
		CloudProvider:      provider,
		Region:             spec.Region,
		RoleARN:            spec.RoleARN,
		RoleSessionName:    spec.RoleSessionName,
		ExternalID:         spec.ExternalID,
		AliyunRoleAuthMode: spec.AliyunRoleAuthMode,
		LoadFrequency:      spec.LoadFrequency,
		BucketName:         location.Bucket,
		RootPath:           location.RootPath,
	}
	if provider == oss.CloudProviderAliyun && param.RoleARN != "" && param.AliyunRoleAuthMode == "" {
		param.AliyunRoleAuthMode = "oidc"
	}
	if param.RoleARN == "" && param.AK == "" && param.SK == "" && !param.Anonymous {
		param.UseIAM = true
	}
	oss.WithSkipCheckBucket(skipBucketCheck)(&param)
	return param, location, nil
}

func parseLegacyExternalSource(raw string) (externalSourceLocation, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return externalSourceLocation{}, errors.Wrap(err, "parse external source")
	}
	if u.Scheme == "" || u.Host == "" {
		return externalSourceLocation{}, errors.Newf(
			"invalid external source: %s", redactExternalVectorObjectKey(raw))
	}
	parts := strings.Split(strings.Trim(strings.TrimSpace(u.Path), "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return externalSourceLocation{}, errors.Newf(
			"external source %s does not include bucket", redactExternalVectorObjectKey(raw))
	}
	location := externalSourceLocation{
		Scheme:     strings.ToLower(u.Scheme),
		SourceHost: u.Host,
		Host:       u.Host,
		Bucket:     parts[0],
	}
	if len(parts) > 1 {
		location.RootPath = cleanExternalRootPath(path.Join(parts[1:]...))
	}
	return location, nil
}

func inferLegacyCloudProviderFromScheme(scheme string) string {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case externalspec.SchemeOSS:
		return oss.CloudProviderAliyun
	case externalspec.SchemeS3, externalspec.SchemeS3A, externalspec.SchemeAWS:
		return oss.CloudProviderAWS
	case externalspec.SchemeGS, externalspec.SchemeGCS:
		return oss.CloudProviderGCP
	case externalspec.SchemeMinIO:
		return oss.CloudProviderMinio
	default:
		return ""
	}
}

func cleanExternalRootPath(value string) string {
	cleaned := path.Clean(strings.Trim(value, "/"))
	if cleaned == "." {
		return ""
	}
	return cleaned
}

func resolveExternalObjectKey(location externalSourceLocation, externalFile string) (string, error) {
	trimmed := strings.TrimSpace(externalFile)
	if trimmed == "" {
		return "", errors.New("external file path is empty")
	}
	if strings.Contains(trimmed, "ROOT_PATH") {
		return strings.ReplaceAll(trimmed, "ROOT_PATH", location.RootPath), nil
	}
	if strings.Contains(trimmed, "://") {
		u, err := url.Parse(trimmed)
		if err != nil {
			return "", errors.Wrap(err, "parse external file path")
		}
		if location.HostIsBucket {
			if u.Host != location.Bucket {
				return "", errors.Newf(
					"external file bucket %s does not match external source bucket %s",
					u.Host,
					location.Bucket,
				)
			}
			trimmed = strings.Trim(u.Path, "/")
		} else {
			parts := strings.Split(strings.Trim(u.Path, "/"), "/")
			if len(parts) == 0 || parts[0] == "" {
				return "", errors.Newf(
					"external file path %s does not include bucket",
					redactExternalVectorObjectKey(trimmed),
				)
			}
			if u.Host != location.SourceHost {
				return "", errors.Newf(
					"external file host %s does not match external source host %s",
					u.Host,
					location.SourceHost,
				)
			}
			if parts[0] != location.Bucket {
				return "", errors.Newf(
					"external file bucket %s does not match external source bucket %s",
					parts[0],
					location.Bucket,
				)
			}
			trimmed = path.Join(parts[1:]...)
		}
	}
	trimmed = strings.TrimPrefix(trimmed, "/")
	if location.RootPath == "" {
		return path.Clean(trimmed), nil
	}
	if trimmed == location.RootPath || strings.HasPrefix(trimmed, location.RootPath+"/") {
		return path.Clean(trimmed), nil
	}
	return path.Join(location.RootPath, trimmed), nil
}
