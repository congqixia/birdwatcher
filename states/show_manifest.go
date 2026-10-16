package states

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/ossutil"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

type ShowManifestParam struct {
	OSSOverrideParam
	framework.ParamBase `use:"show manifest" desc:"parse and display manifest file from S3 for segments"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	SegmentID           int64  `name:"segment" default:"0" desc:"segment id to display"`
	MinioAddress        string `name:"minioAddr" desc:"override minio address"`
	SkipBucketCheck     bool   `name:"skipBucketCheck" default:"false" desc:"skip bucket exist check"`
	JSONOutput          bool   `name:"json" default:"false" desc:"output as JSON"`
}

func (s *InstanceState) ShowManifestCommand(ctx context.Context, p *ShowManifestParam) error {
	if p.CollectionID == 0 && p.SegmentID == 0 {
		return fmt.Errorf("at least one of --collection or --segment must be specified")
	}

	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(seg *models.Segment) bool {
		return (p.CollectionID == 0 || seg.CollectionID == p.CollectionID) &&
			(p.SegmentID == 0 || seg.ID == p.SegmentID) &&
			seg.State != commonpb.SegmentState_Dropped
	})
	if err != nil {
		return err
	}

	// Filter segments that have manifest paths
	var manifestSegments []*models.Segment
	for _, seg := range segments {
		if seg.GetManifestPath() != "" {
			manifestSegments = append(manifestSegments, seg)
		}
	}

	if len(manifestSegments) == 0 {
		fmt.Println("No segments with manifest path found")
		return nil
	}

	resolvedStore, err := s.getObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, p.SkipBucketCheck)
	if err != nil {
		return fmt.Errorf("failed to create minio client: %w", err)
	}
	rootPath := resolvedStore.RootPath

	// External collections: parse the external source/spec into a location
	// WITHOUT connecting to the external bucket. show manifest only reads the
	// manifest from Milvus internal storage and uses the location to annotate
	// paths, so it must not fail when external credentials or connectivity are
	// broken. The external client is created lazily only by commands that
	// actually read external objects (scan-binlog, inspect-parquet).
	// Resolve the collection by --collection, or fall back to the first manifest
	// segment when only --segment is given.
	var externalLocation ossutil.ExternalSourceLocation
	var externalResolved bool
	collectionID := p.CollectionID
	if collectionID == 0 && len(manifestSegments) > 0 {
		collectionID = manifestSegments[0].CollectionID
	}
	if collectionID != 0 {
		collection, err := common.GetCollectionByIDVersion(ctx, s.client, s.basePath, collectionID)
		if err != nil {
			return err
		}
		if collection.GetProto().GetSchema().GetExternalSource() != "" {
			externalLocation, err = ossutil.ResolveExternalLocationFromCollection(collection)
			if err != nil {
				return fmt.Errorf("failed to parse external location: %w", err)
			}
			externalResolved = true
		}
	}

	for _, seg := range manifestSegments {
		rawManifest := seg.GetManifestPath()
		fmt.Printf("=== Segment %d (Collection: %d, Partition: %d) ===\n", seg.ID, seg.CollectionID, seg.PartitionID)
		fmt.Printf("Manifest Raw: %s\n", rawManifest)

		// ManifestPath is JSON: {"ver":2,"base_path":"files/insert_log/..."}
		// Actual file: {rootPath}/{base_path}/_metadata/manifest-{ver}.arvo
		var manifestRef struct {
			Ver      int    `json:"ver"`
			BasePath string `json:"base_path"`
		}
		if err := json.Unmarshal([]byte(rawManifest), &manifestRef); err != nil {
			fmt.Printf("Error parsing manifest path JSON: %v\n\n", err)
			continue
		}

		basePath := oss.ResolveObjectKey(rootPath, manifestRef.BasePath)
		manifestPath := path.Join(basePath, "_metadata", fmt.Sprintf("manifest-%d.avro", manifestRef.Ver))
		fmt.Printf("Manifest File: %s\n", manifestPath)

		obj, err := resolvedStore.Store.Open(ctx, manifestPath)
		if err != nil {
			fmt.Printf("Error getting object: %v\n\n", err)
			continue
		}

		manifest, err := parseManifest(obj)
		if err != nil {
			fmt.Printf("Error parsing manifest: %v\n\n", err)
			continue
		}

		switch {
		case p.JSONOutput:
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			if err := enc.Encode(manifest); err != nil {
				fmt.Printf("Error encoding JSON: %v\n", err)
			}
		case externalResolved && seg.CollectionID == collectionID:
			// Annotate paths with storage backend / object key. The resolver
			// only annotates; it never opens external objects, so no external
			// store is built here.
			resolver := ossutil.NewManifestPathResolver(resolvedStore.Store, basePath, nil, externalLocation)
			printManifest(manifest, resolver)
		default:
			printManifest(manifest, nil)
		}
		fmt.Println()
	}

	return nil
}

// Manifest parsing types and logic (Avro binary encoding)

const manifestMagic int32 = 0x4D494C56 // "MILV"

// Manifest format evolution:
//   - v1: initial (column_groups, delta_logs, stats as map<string, array<string>>)
//   - v2: added indexes
//   - v3: stats changed to map<string, Statistics>
//   - v4: ColumnGroupFile.metadata (bytes) replaced by properties (map<string,string>)
//   - v5: added lob_files
//   - v6: expanded Index with typed artifact metadata
const (
	manifestVersionV3 int32 = 3
	manifestVersionV4 int32 = 4
	manifestVersionV5 int32 = 5
	manifestVersionV6 int32 = 6
)

// avroOCFMagic is the 4-byte magic header for Avro Object Container Format files.
var avroOCFMagic = []byte{'O', 'b', 'j', 0x01}

type manifestColumnGroupFile struct {
	Path       string            `json:"path"`
	StartIndex int64             `json:"start_index"`
	EndIndex   int64             `json:"end_index"`
	Metadata   []byte            `json:"metadata,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

type manifestColumnGroup struct {
	Columns []string                  `json:"columns"`
	Files   []manifestColumnGroupFile `json:"files"`
	Format  string                    `json:"format"`
}

type manifestDeltaLogType int32

const (
	manifestDeltaLogTypePrimaryKey manifestDeltaLogType = 0
	manifestDeltaLogTypePositional manifestDeltaLogType = 1
	manifestDeltaLogTypeEquality   manifestDeltaLogType = 2
)

func (t manifestDeltaLogType) String() string {
	switch t {
	case manifestDeltaLogTypePrimaryKey:
		return "PRIMARY_KEY"
	case manifestDeltaLogTypePositional:
		return "POSITIONAL"
	case manifestDeltaLogTypeEquality:
		return "EQUALITY"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int32(t))
	}
}

func (t manifestDeltaLogType) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

type manifestDeltaLog struct {
	Path       string               `json:"path"`
	Type       manifestDeltaLogType `json:"type"`
	NumEntries int64                `json:"num_entries"`
}

// manifestIndex represents index metadata for a column.
type manifestIndex struct {
	ColumnName                string            `json:"column_name"`
	IndexName                 string            `json:"index_name,omitempty"`
	IndexType                 string            `json:"index_type"`
	Path                      string            `json:"path"`
	FieldID                   int64             `json:"field_id,omitempty"`
	IndexID                   int64             `json:"index_id,omitempty"`
	BuildID                   int64             `json:"build_id,omitempty"`
	IndexVersion              int64             `json:"index_version,omitempty"`
	NumRows                   int64             `json:"num_rows,omitempty"`
	SerializedSize            int64             `json:"serialized_size,omitempty"`
	MemSize                   int64             `json:"mem_size,omitempty"`
	CurrentIndexVersion       int32             `json:"current_index_version,omitempty"`
	CurrentScalarIndexVersion int32             `json:"current_scalar_index_version,omitempty"`
	IndexStorePathVersion     int32             `json:"index_store_path_version,omitempty"`
	IndexFileKeys             []string          `json:"index_file_keys,omitempty"`
	Properties                map[string]string `json:"properties,omitempty"`
}

type manifestLobFileInfo struct {
	Path          string `json:"path"`
	FieldID       int64  `json:"field_id"`
	TotalRows     int64  `json:"total_rows"`
	ValidRows     int64  `json:"valid_rows"`
	FileSizeBytes int64  `json:"file_size_bytes"`
}

// manifestStatistics represents a stats entry with file paths and optional metadata.
type manifestStatistics struct {
	Paths    []string          `json:"paths"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type manifest struct {
	Format       string                         `json:"format"`
	Magic        int32                          `json:"magic,omitempty"`
	MagicStr     string                         `json:"magic_str,omitempty"`
	Version      int32                          `json:"version"`
	ColumnGroups []manifestColumnGroup          `json:"column_groups"`
	DeltaLogs    []manifestDeltaLog             `json:"delta_logs"`
	Stats        map[string]*manifestStatistics `json:"stats"`
	Indexes      []manifestIndex                `json:"indexes,omitempty"`
	LobFiles     []manifestLobFileInfo          `json:"lob_files,omitempty"`
}

// avroReader wraps an io.Reader to decode Avro binary encoding primitives.
type avroReader struct {
	r io.Reader
}

func (a *avroReader) readByte() (byte, error) {
	var buf [1]byte
	_, err := io.ReadFull(a.r, buf[:])
	return buf[0], err
}

func (a *avroReader) readLong() (int64, error) {
	var val uint64
	var shift uint
	for {
		b, err := a.readByte()
		if err != nil {
			return 0, fmt.Errorf("readLong: %w", err)
		}
		val |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return int64(val>>1) ^ -int64(val&1), nil
}

func (a *avroReader) readInt() (int32, error) {
	v, err := a.readLong()
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

func (a *avroReader) readString() (string, error) {
	length, err := a.readLong()
	if err != nil {
		return "", fmt.Errorf("readString length: %w", err)
	}
	if length < 0 {
		return "", fmt.Errorf("readString: negative length %d", length)
	}
	if length == 0 {
		return "", nil
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(a.r, buf)
	if err != nil {
		return "", fmt.Errorf("readString data: %w", err)
	}
	return string(buf), nil
}

func (a *avroReader) readBytes() ([]byte, error) {
	length, err := a.readLong()
	if err != nil {
		return nil, fmt.Errorf("readBytes length: %w", err)
	}
	if length < 0 {
		return nil, fmt.Errorf("readBytes: negative length %d", length)
	}
	if length == 0 {
		return nil, nil
	}
	buf := make([]byte, length)
	_, err = io.ReadFull(a.r, buf)
	if err != nil {
		return nil, fmt.Errorf("readBytes data: %w", err)
	}
	return buf, nil
}

func (a *avroReader) readArrayBlockCount() (int64, error) {
	count, err := a.readLong()
	if err != nil {
		return 0, err
	}
	if count < 0 {
		count = -count
		if _, err := a.readLong(); err != nil {
			return 0, fmt.Errorf("readArrayBlockCount byte-size: %w", err)
		}
	}
	return count, nil
}

func readAvroArray[T any](a *avroReader, readElem func() (T, error)) ([]T, error) {
	var result []T
	for {
		count, err := a.readArrayBlockCount()
		if err != nil {
			return nil, err
		}
		if count == 0 {
			break
		}
		for i := int64(0); i < count; i++ {
			elem, err := readElem()
			if err != nil {
				return nil, err
			}
			result = append(result, elem)
		}
	}
	return result, nil
}

func readAvroMap[V any](a *avroReader, readValue func() (V, error)) (map[string]V, error) {
	result := make(map[string]V)
	for {
		count, err := a.readArrayBlockCount()
		if err != nil {
			return nil, err
		}
		if count == 0 {
			break
		}
		for i := int64(0); i < count; i++ {
			key, err := a.readString()
			if err != nil {
				return nil, fmt.Errorf("map key: %w", err)
			}
			val, err := readValue()
			if err != nil {
				return nil, fmt.Errorf("map value for key %q: %w", key, err)
			}
			result[key] = val
		}
	}
	return result, nil
}

func (a *avroReader) readColumnGroupFile(hasProperties bool) (manifestColumnGroupFile, error) {
	var f manifestColumnGroupFile
	var err error
	if f.Path, err = a.readString(); err != nil {
		return f, fmt.Errorf("ColumnGroupFile.path: %w", err)
	}
	if f.StartIndex, err = a.readLong(); err != nil {
		return f, fmt.Errorf("ColumnGroupFile.start_index: %w", err)
	}
	if f.EndIndex, err = a.readLong(); err != nil {
		return f, fmt.Errorf("ColumnGroupFile.end_index: %w", err)
	}
	if hasProperties {
		f.Properties, err = readAvroMap(a, func() (string, error) {
			return a.readString()
		})
		if err != nil {
			return f, fmt.Errorf("ColumnGroupFile.properties: %w", err)
		}
	} else {
		if f.Metadata, err = a.readBytes(); err != nil {
			return f, fmt.Errorf("ColumnGroupFile.metadata: %w", err)
		}
	}
	return f, nil
}

func (a *avroReader) readColumnGroup(hasFileProperties bool) (manifestColumnGroup, error) {
	var cg manifestColumnGroup
	var err error

	cg.Columns, err = readAvroArray(a, func() (string, error) {
		return a.readString()
	})
	if err != nil {
		return cg, fmt.Errorf("ColumnGroup.columns: %w", err)
	}

	cg.Files, err = readAvroArray(a, func() (manifestColumnGroupFile, error) {
		return a.readColumnGroupFile(hasFileProperties)
	})
	if err != nil {
		return cg, fmt.Errorf("ColumnGroup.files: %w", err)
	}

	cg.Format, err = a.readString()
	if err != nil {
		return cg, fmt.Errorf("ColumnGroup.format: %w", err)
	}
	return cg, nil
}

func (a *avroReader) readDeltaLog() (manifestDeltaLog, error) {
	var dl manifestDeltaLog
	var err error
	if dl.Path, err = a.readString(); err != nil {
		return dl, fmt.Errorf("DeltaLog.path: %w", err)
	}
	typeInt, err := a.readInt()
	if err != nil {
		return dl, fmt.Errorf("DeltaLog.type: %w", err)
	}
	dl.Type = manifestDeltaLogType(typeInt)
	if dl.NumEntries, err = a.readLong(); err != nil {
		return dl, fmt.Errorf("DeltaLog.num_entries: %w", err)
	}
	return dl, nil
}

// readIndex decodes a single Index. Version 6 added typed artifact metadata
// between column_name and properties; earlier versions use the original four fields.
func (a *avroReader) readIndex(hasTypedMetadata bool) (manifestIndex, error) {
	var idx manifestIndex
	var err error
	if idx.ColumnName, err = a.readString(); err != nil {
		return idx, fmt.Errorf("index.column_name: %w", err)
	}
	if hasTypedMetadata {
		if idx.IndexName, err = a.readString(); err != nil {
			return idx, fmt.Errorf("index.index_name: %w", err)
		}
	}
	if idx.IndexType, err = a.readString(); err != nil {
		return idx, fmt.Errorf("index.index_type: %w", err)
	}
	if idx.Path, err = a.readString(); err != nil {
		return idx, fmt.Errorf("index.path: %w", err)
	}
	if hasTypedMetadata {
		if idx.FieldID, err = a.readLong(); err != nil {
			return idx, fmt.Errorf("index.field_id: %w", err)
		}
		if idx.IndexID, err = a.readLong(); err != nil {
			return idx, fmt.Errorf("index.index_id: %w", err)
		}
		if idx.BuildID, err = a.readLong(); err != nil {
			return idx, fmt.Errorf("index.build_id: %w", err)
		}
		if idx.IndexVersion, err = a.readLong(); err != nil {
			return idx, fmt.Errorf("index.index_version: %w", err)
		}
		if idx.NumRows, err = a.readLong(); err != nil {
			return idx, fmt.Errorf("index.num_rows: %w", err)
		}
		if idx.SerializedSize, err = a.readLong(); err != nil {
			return idx, fmt.Errorf("index.serialized_size: %w", err)
		}
		if idx.MemSize, err = a.readLong(); err != nil {
			return idx, fmt.Errorf("index.mem_size: %w", err)
		}
		if idx.CurrentIndexVersion, err = a.readInt(); err != nil {
			return idx, fmt.Errorf("index.current_index_version: %w", err)
		}
		if idx.CurrentScalarIndexVersion, err = a.readInt(); err != nil {
			return idx, fmt.Errorf("index.current_scalar_index_version: %w", err)
		}
		if idx.IndexStorePathVersion, err = a.readInt(); err != nil {
			return idx, fmt.Errorf("index.index_store_path_version: %w", err)
		}
		idx.IndexFileKeys, err = readAvroArray(a, func() (string, error) {
			return a.readString()
		})
		if err != nil {
			return idx, fmt.Errorf("index.index_file_keys: %w", err)
		}
	}
	idx.Properties, err = readAvroMap(a, func() (string, error) {
		return a.readString()
	})
	if err != nil {
		return idx, fmt.Errorf("index.properties: %w", err)
	}
	return idx, nil
}

func (a *avroReader) readLobFileInfo() (manifestLobFileInfo, error) {
	var lob manifestLobFileInfo
	var err error
	if lob.Path, err = a.readString(); err != nil {
		return lob, fmt.Errorf("lob_file.path: %w", err)
	}
	if lob.FieldID, err = a.readLong(); err != nil {
		return lob, fmt.Errorf("lob_file.field_id: %w", err)
	}
	if lob.TotalRows, err = a.readLong(); err != nil {
		return lob, fmt.Errorf("lob_file.total_rows: %w", err)
	}
	if lob.ValidRows, err = a.readLong(); err != nil {
		return lob, fmt.Errorf("lob_file.valid_rows: %w", err)
	}
	if lob.FileSizeBytes, err = a.readLong(); err != nil {
		return lob, fmt.Errorf("lob_file.file_size_bytes: %w", err)
	}
	return lob, nil
}

// readStatistics decodes a single Statistics (v3 format).
// Encoding order: paths(array<string>), metadata(map<string,string>)
func (a *avroReader) readStatistics() (manifestStatistics, error) {
	var stat manifestStatistics
	var err error
	stat.Paths, err = readAvroArray(a, func() (string, error) {
		return a.readString()
	})
	if err != nil {
		return stat, fmt.Errorf("statistics.paths: %w", err)
	}
	stat.Metadata, err = readAvroMap(a, func() (string, error) {
		return a.readString()
	})
	if err != nil {
		return stat, fmt.Errorf("statistics.metadata: %w", err)
	}
	return stat, nil
}

// readManifestRecord decodes the Manifest record fields from Avro binary encoding.
// Field order: column_groups, delta_logs, stats(map<string, Statistics>), indexes, lob_files
func readManifestRecord(ar *avroReader, version int32) (*manifest, error) {
	m := &manifest{}
	var err error

	// 1. Column groups: array<ColumnGroup>
	m.ColumnGroups, err = readAvroArray(ar, func() (manifestColumnGroup, error) {
		return ar.readColumnGroup(version >= manifestVersionV4)
	})
	if err != nil {
		return nil, fmt.Errorf("reading column_groups: %w", err)
	}

	// 2. Delta logs: array<DeltaLog>
	m.DeltaLogs, err = readAvroArray(ar, func() (manifestDeltaLog, error) {
		return ar.readDeltaLog()
	})
	if err != nil {
		return nil, fmt.Errorf("reading delta_logs: %w", err)
	}

	// 3. Stats: map<string, Statistics>
	m.Stats, err = readAvroMap(ar, func() (*manifestStatistics, error) {
		stat, err := ar.readStatistics()
		if err != nil {
			return nil, err
		}
		return &stat, nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading stats: %w", err)
	}

	// 4. Indexes: array<Index>
	m.Indexes, err = readAvroArray(ar, func() (manifestIndex, error) {
		return ar.readIndex(version >= manifestVersionV6)
	})
	if err != nil {
		return nil, fmt.Errorf("reading indexes: %w", err)
	}

	// 5. LOB files: array<LobFileInfo> (v5+)
	if version >= manifestVersionV5 {
		m.LobFiles, err = readAvroArray(ar, func() (manifestLobFileInfo, error) {
			return ar.readLobFileInfo()
		})
		if err != nil {
			return nil, fmt.Errorf("reading lob_files: %w", err)
		}
	}

	return m, nil
}

// parseAvroOCF parses an Avro Object Container Format file.
// The reader should be positioned right after the 4-byte "Obj\x01" magic.
func parseAvroOCF(r io.Reader) (*manifest, error) {
	ar := &avroReader{r: r}

	// Read file metadata: map<string, bytes>
	meta, err := readAvroMap(ar, func() ([]byte, error) {
		return ar.readBytes()
	})
	if err != nil {
		return nil, fmt.Errorf("reading OCF metadata: %w", err)
	}

	// Extract codec (default "null")
	codec := "null"
	if codecBytes, ok := meta["avro.codec"]; ok {
		codec = string(codecBytes)
	}

	version := manifestVersionV3
	if schemaBytes, ok := meta["avro.schema"]; ok {
		version = detectOCFManifestVersion(schemaBytes)
	}

	// Read 16-byte sync marker
	var syncMarker [16]byte
	if _, err := io.ReadFull(r, syncMarker[:]); err != nil {
		return nil, fmt.Errorf("reading sync marker: %w", err)
	}

	// Read data blocks until EOF
	var allData []byte
	for {
		// Read object count (long)
		objectCount, err := ar.readLong()
		if err != nil {
			// EOF means no more blocks
			break
		}
		if objectCount <= 0 {
			break
		}

		// Read block byte size (long)
		blockSize, err := ar.readLong()
		if err != nil {
			return nil, fmt.Errorf("reading block size: %w", err)
		}

		// Read block data
		blockData := make([]byte, blockSize)
		if _, err := io.ReadFull(r, blockData); err != nil {
			return nil, fmt.Errorf("reading block data: %w", err)
		}

		// Decompress if needed
		switch codec {
		case "null":
			allData = append(allData, blockData...)
		case "deflate":
			fr := flate.NewReader(bytes.NewReader(blockData))
			decompressed, err := io.ReadAll(fr)
			fr.Close()
			if err != nil {
				return nil, fmt.Errorf("deflate decompression: %w", err)
			}
			allData = append(allData, decompressed...)
		case "snappy":
			decompressed, err := manifestSnappyDecode(blockData)
			if err != nil {
				return nil, fmt.Errorf("snappy decompression: %w", err)
			}
			allData = append(allData, decompressed...)
		default:
			return nil, fmt.Errorf("unsupported codec: %s", codec)
		}

		// Read and verify sync marker
		var blockSync [16]byte
		if _, err := io.ReadFull(r, blockSync[:]); err != nil {
			return nil, fmt.Errorf("reading block sync marker: %w", err)
		}
		if blockSync != syncMarker {
			return nil, fmt.Errorf("sync marker mismatch")
		}
	}

	if len(allData) == 0 {
		return nil, fmt.Errorf("no data blocks found in OCF file")
	}

	// Decode the manifest record from the accumulated block data
	blockReader := &avroReader{r: bytes.NewReader(allData)}
	m, err := readManifestRecord(blockReader, version)
	if err != nil {
		return nil, fmt.Errorf("decoding manifest record: %w", err)
	}

	m.Format = "avro_ocf"
	m.Version = version

	return m, nil
}

// detectOCFManifestVersion infers the writer version from fields added by each OCF schema revision.
func detectOCFManifestVersion(schemaJSON []byte) int32 {
	var schema any
	if err := json.Unmarshal(schemaJSON, &schema); err != nil {
		return manifestVersionV3
	}
	switch {
	case avroRecordHasField(schema, "Index", "index_name"):
		return manifestVersionV6
	case avroRecordHasField(schema, "Manifest", "lob_files"):
		return manifestVersionV5
	case avroRecordHasField(schema, "ColumnGroupFile", "properties"):
		return manifestVersionV4
	default:
		return manifestVersionV3
	}
}

func avroRecordHasField(node any, recordName, fieldName string) bool {
	switch value := node.(type) {
	case map[string]any:
		if name, _ := value["name"].(string); name == recordName {
			if fields, ok := value["fields"].([]any); ok {
				for _, field := range fields {
					fieldMap, ok := field.(map[string]any)
					if !ok {
						continue
					}
					if name, _ := fieldMap["name"].(string); name == fieldName {
						return true
					}
				}
			}
		}
		for _, child := range value {
			if avroRecordHasField(child, recordName, fieldName) {
				return true
			}
		}
	case []any:
		for _, child := range value {
			if avroRecordHasField(child, recordName, fieldName) {
				return true
			}
		}
	}
	return false
}

// manifestSnappyDecode decodes Avro-framed snappy data.
// Avro uses snappy block format: compressed data followed by a 4-byte CRC32C checksum of the uncompressed data.
func manifestSnappyDecode(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("snappy block too short: %d bytes", len(data))
	}

	// The last 4 bytes are a CRC32C checksum of the uncompressed data
	compressedData := data[:len(data)-4]
	expectedCRC := binary.BigEndian.Uint32(data[len(data)-4:])

	decoded, err := manifestSnappyDecodeBlock(compressedData)
	if err != nil {
		return nil, err
	}

	// Verify CRC32C checksum
	actualCRC := crc32.Checksum(decoded, crc32.MakeTable(crc32.Castagnoli))
	if actualCRC != expectedCRC {
		return nil, fmt.Errorf("snappy CRC32C mismatch: expected 0x%08x, got 0x%08x", expectedCRC, actualCRC)
	}

	return decoded, nil
}

// manifestSnappyDecodeBlock decodes a raw snappy-compressed block.
func manifestSnappyDecodeBlock(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}

	// Read preamble: uncompressed length as varint
	dLen, n := manifestDecodeVarint(src)
	if n <= 0 {
		return nil, fmt.Errorf("snappy: invalid varint in preamble")
	}
	src = src[n:]

	dst := make([]byte, 0, dLen)

	for len(src) > 0 {
		tag := src[0]
		tagType := tag & 0x03

		switch tagType {
		case 0: // Literal
			litLen := int(tag>>2) + 1
			src = src[1:]
			switch litLen {
			case 60 + 1:
				if len(src) < 1 {
					return nil, fmt.Errorf("snappy: truncated literal length")
				}
				litLen = int(src[0]) + 1
				src = src[1:]
			case 61 + 1:
				if len(src) < 2 {
					return nil, fmt.Errorf("snappy: truncated literal length")
				}
				litLen = int(src[0]) | int(src[1])<<8 + 1
				src = src[2:]
			case 62 + 1:
				if len(src) < 3 {
					return nil, fmt.Errorf("snappy: truncated literal length")
				}
				litLen = int(src[0]) | int(src[1])<<8 | int(src[2])<<16 + 1
				src = src[3:]
			case 63 + 1:
				if len(src) < 4 {
					return nil, fmt.Errorf("snappy: truncated literal length")
				}
				litLen = int(src[0]) | int(src[1])<<8 | int(src[2])<<16 | int(src[3])<<24 + 1
				src = src[4:]
			}
			if len(src) < litLen {
				return nil, fmt.Errorf("snappy: truncated literal data")
			}
			dst = append(dst, src[:litLen]...)
			src = src[litLen:]

		case 1: // Copy with 1-byte offset
			length := int(tag>>2)&0x07 + 4
			if len(src) < 2 {
				return nil, fmt.Errorf("snappy: truncated copy1")
			}
			offset := int(tag)&0xe0<<3 | int(src[1])
			src = src[2:]
			if offset == 0 || offset > len(dst) {
				return nil, fmt.Errorf("snappy: invalid copy1 offset %d (output length %d)", offset, len(dst))
			}
			for i := 0; i < length; i++ {
				dst = append(dst, dst[len(dst)-offset])
			}

		case 2: // Copy with 2-byte offset
			length := int(tag>>2) + 1
			if len(src) < 3 {
				return nil, fmt.Errorf("snappy: truncated copy2")
			}
			offset := int(src[1]) | int(src[2])<<8
			src = src[3:]
			if offset == 0 || offset > len(dst) {
				return nil, fmt.Errorf("snappy: invalid copy2 offset %d (output length %d)", offset, len(dst))
			}
			for i := 0; i < length; i++ {
				dst = append(dst, dst[len(dst)-offset])
			}

		case 3: // Copy with 4-byte offset
			length := int(tag>>2) + 1
			if len(src) < 5 {
				return nil, fmt.Errorf("snappy: truncated copy4")
			}
			offset := int(src[1]) | int(src[2])<<8 | int(src[3])<<16 | int(src[4])<<24
			src = src[5:]
			if offset == 0 || offset > len(dst) {
				return nil, fmt.Errorf("snappy: invalid copy4 offset %d (output length %d)", offset, len(dst))
			}
			for i := 0; i < length; i++ {
				dst = append(dst, dst[len(dst)-offset])
			}
		}
	}

	if uint64(len(dst)) != dLen {
		return nil, fmt.Errorf("snappy: output length mismatch: got %d, expected %d", len(dst), dLen)
	}
	return dst, nil
}

// manifestDecodeVarint decodes a little-endian base-128 varint.
func manifestDecodeVarint(buf []byte) (uint64, int) {
	var val uint64
	var shift uint
	for i, b := range buf {
		val |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return val, i + 1
		}
		shift += 7
		if shift >= 64 {
			return 0, -1
		}
	}
	return 0, -1
}

// parseLegacyManifest parses the legacy MILV format (raw Avro binary with magic + version prefix).
func parseLegacyManifest(r io.Reader) (*manifest, error) {
	ar := &avroReader{r: r}
	m := &manifest{Format: "legacy_milv"}
	var err error

	// 1. Magic
	m.Magic, err = ar.readInt()
	if err != nil {
		return nil, fmt.Errorf("reading magic: %w", err)
	}
	var magicBytes [4]byte
	binary.BigEndian.PutUint32(magicBytes[:], uint32(m.Magic))
	m.MagicStr = string(magicBytes[:])

	if m.Magic != manifestMagic {
		return nil, fmt.Errorf("invalid magic number: expected 0x%08X (%q), got 0x%08X (%q)",
			manifestMagic, "MILV", m.Magic, m.MagicStr)
	}

	// 2. Version
	m.Version, err = ar.readInt()
	if err != nil {
		return nil, fmt.Errorf("reading version: %w", err)
	}
	if m.Version < 1 || (m.Version > manifestVersionV3 && m.Version != manifestVersionV6) {
		return nil, fmt.Errorf("unsupported manifest version: %d (expected 1-3 or %d)", m.Version, manifestVersionV6)
	}

	// 3. Column groups: array<ColumnGroup>
	// Supported raw MILV versions keep the legacy metadata (bytes) layout.
	m.ColumnGroups, err = readAvroArray(ar, func() (manifestColumnGroup, error) {
		return ar.readColumnGroup(false)
	})
	if err != nil {
		return nil, fmt.Errorf("reading column_groups: %w", err)
	}

	// 4. Delta logs: array<DeltaLog>
	m.DeltaLogs, err = readAvroArray(ar, func() (manifestDeltaLog, error) {
		return ar.readDeltaLog()
	})
	if err != nil {
		return nil, fmt.Errorf("reading delta_logs: %w", err)
	}

	// 5. Stats: version-dependent format
	if m.Version >= manifestVersionV3 {
		// v3: map<string, Statistics> where Statistics = {paths: array<string>, metadata: map<string,string>}
		m.Stats, err = readAvroMap(ar, func() (*manifestStatistics, error) {
			stat, err := ar.readStatistics()
			if err != nil {
				return nil, err
			}
			return &stat, nil
		})
		if err != nil {
			return nil, fmt.Errorf("reading stats (v3): %w", err)
		}
	} else {
		// v1/v2: map<string, array<string>> — convert to manifestStatistics with empty metadata
		legacyStats, err := readAvroMap(ar, func() ([]string, error) {
			return readAvroArray(ar, func() (string, error) {
				return ar.readString()
			})
		})
		if err != nil {
			return nil, fmt.Errorf("reading stats (legacy): %w", err)
		}
		m.Stats = make(map[string]*manifestStatistics, len(legacyStats))
		for k, paths := range legacyStats {
			m.Stats[k] = &manifestStatistics{Paths: paths}
		}
	}

	// 6. Indexes: v2-v3 use the original layout; v6 uses typed metadata.
	if m.Version >= 2 {
		m.Indexes, err = readAvroArray(ar, func() (manifestIndex, error) {
			return ar.readIndex(m.Version == manifestVersionV6)
		})
		if err != nil {
			return nil, fmt.Errorf("reading indexes: %w", err)
		}
	}

	// 7. LOB files: array<LobFileInfo> (v6)
	if m.Version == manifestVersionV6 {
		m.LobFiles, err = readAvroArray(ar, func() (manifestLobFileInfo, error) {
			return ar.readLobFileInfo()
		})
		if err != nil {
			return nil, fmt.Errorf("reading lob_files: %w", err)
		}
	}

	return m, nil
}

// parseManifest detects the format and parses accordingly.
func parseManifest(r io.ReadSeeker) (*manifest, error) {
	// Read first 4 bytes to detect format
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}

	// Seek back to start
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seeking to start: %w", err)
	}

	if bytes.Equal(header[:], avroOCFMagic) {
		// Avro Object Container Format — skip the 4-byte magic
		if _, err := r.Seek(4, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seeking past OCF magic: %w", err)
		}
		return parseAvroOCF(r)
	}

	// Legacy MILV format
	return parseLegacyManifest(r)
}

// printManifest prints manifest entries. When resolver is non-nil, each file
// path is annotated with the storage backend and resolved object key; otherwise
// the raw paths are printed as stored. All existing diagnostic output (column
// group metadata, stats metadata, V6 index details, index file keys, index
// properties) is preserved in both modes.
func printManifest(m *manifest, resolver *ossutil.ManifestPathResolver) {
	fmt.Printf("Format:  %s\n", m.Format)
	if m.Format == "legacy_milv" {
		fmt.Printf("Magic:   0x%08X (%q)\n", m.Magic, m.MagicStr)
	}
	fmt.Printf("Version: %d\n", m.Version)

	fmt.Printf("\nColumn Groups (%d):\n", len(m.ColumnGroups))
	for i, cg := range m.ColumnGroups {
		fmt.Printf("\n  --- Column Group #%d ---\n", i)
		fmt.Printf("  Format:  %s\n", cg.Format)
		fmt.Printf("  Columns: %v\n", cg.Columns)
		fmt.Printf("  Files (%d):\n", len(cg.Files))
		for j, f := range cg.Files {
			fmt.Printf("    [%d] Path: %s\n", j, f.Path)
			fmt.Printf("        Range: [%d, %d)\n", f.StartIndex, f.EndIndex)
			if resolver != nil {
				_, key, backend, err := resolver.Resolve(f.Path, "_data")
				if err != nil {
					fmt.Printf("        Resolve Error: %v\n", err)
				} else {
					fmt.Printf("        Backend: %s  Object Key: %s\n", backend, key)
				}
			}
			if len(f.Metadata) > 0 {
				fmt.Printf("        Metadata (%d bytes): %s\n", len(f.Metadata), hex.EncodeToString(f.Metadata))
			}
			if len(f.Properties) > 0 {
				fmt.Printf("        Properties:\n")
				for pk, pv := range f.Properties {
					fmt.Printf("          %s: %s\n", pk, pv)
				}
			}
		}
	}

	fmt.Printf("\nDelta Logs (%d):\n", len(m.DeltaLogs))
	for i, dl := range m.DeltaLogs {
		fmt.Printf("  [%d] Path: %s  Type: %s  NumEntries: %d", i, dl.Path, dl.Type, dl.NumEntries)
		if resolver != nil {
			_, key, backend, err := resolver.Resolve(dl.Path, "_delta")
			if err != nil {
				fmt.Printf("  (Resolve Error: %v)", err)
			} else {
				fmt.Printf("  Backend: %s  Object Key: %s", backend, key)
			}
		}
		fmt.Println()
	}

	if len(m.Stats) > 0 {
		fmt.Printf("\nStats (%d keys):\n", len(m.Stats))
		for k, stat := range m.Stats {
			fmt.Printf("  %s:\n", k)
			fmt.Printf("    Paths:\n")
			for _, p := range stat.Paths {
				if resolver != nil {
					_, key, backend, err := resolver.Resolve(p, "_stats")
					if err != nil {
						fmt.Printf("      - %s (Resolve Error: %v)\n", p, err)
					} else {
						fmt.Printf("      - %s  Backend: %s  Object Key: %s\n", p, backend, key)
					}
				} else {
					fmt.Printf("      - %s\n", p)
				}
			}
			if len(stat.Metadata) > 0 {
				fmt.Printf("    Metadata:\n")
				for mk, mv := range stat.Metadata {
					fmt.Printf("      %s: %s\n", mk, mv)
				}
			}
		}
	}

	if len(m.Indexes) > 0 {
		fmt.Printf("\nIndexes (%d):\n", len(m.Indexes))
		for i, idx := range m.Indexes {
			fmt.Printf("  [%d] Column: %s  Type: %s  Path: %s\n", i, idx.ColumnName, idx.IndexType, idx.Path)
			if resolver != nil && idx.Path != "" {
				_, key, backend, err := resolver.Resolve(idx.Path, "_index")
				if err != nil {
					fmt.Printf("      Path Resolve Error: %v\n", err)
				} else {
					fmt.Printf("      Backend: %s  Object Key: %s\n", backend, key)
				}
			}
			if m.Version == manifestVersionV6 {
				fmt.Printf("      Name: %s  FieldID: %d  IndexID: %d  BuildID: %d\n",
					idx.IndexName, idx.FieldID, idx.IndexID, idx.BuildID)
				fmt.Printf("      Version: %d  Rows: %d  SerializedSize: %d  MemSize: %d\n",
					idx.IndexVersion, idx.NumRows, idx.SerializedSize, idx.MemSize)
				fmt.Printf("      EngineVersions: index=%d scalar=%d storePath=%d\n",
					idx.CurrentIndexVersion, idx.CurrentScalarIndexVersion, idx.IndexStorePathVersion)
			}
			if len(idx.IndexFileKeys) > 0 {
				fmt.Printf("      Files: %v\n", idx.IndexFileKeys)
			}
			if len(idx.Properties) > 0 {
				fmt.Printf("      Properties:\n")
				for pk, pv := range idx.Properties {
					fmt.Printf("        %s: %s\n", pk, pv)
				}
			}
		}
	}

	if len(m.LobFiles) > 0 {
		fmt.Printf("\nLOB Files (%d):\n", len(m.LobFiles))
		for i, lob := range m.LobFiles {
			fmt.Printf("  [%d] Path: %s  FieldID: %d  Rows: %d/%d  Size: %d",
				i, lob.Path, lob.FieldID, lob.ValidRows, lob.TotalRows, lob.FileSizeBytes)
			if resolver != nil {
				_, key, backend, err := resolver.Resolve(lob.Path, "../lobs")
				if err != nil {
					fmt.Printf("  (Resolve Error: %v)", err)
				} else {
					fmt.Printf("  Backend: %s  Object Key: %s", backend, key)
				}
			}
			fmt.Println()
		}
	}
}
