package states

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/ossutil"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

type ShowParquetFooterParam struct {
	OSSOverrideParam
	framework.ParamBase `use:"show parquet-footer" desc:"analyze collection segment parquet footer sizes without reading data pages"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection ID (required)"`
	SegmentID           int64  `name:"segment" default:"0" desc:"optional segment ID filter"`
	MinioAddress        string `name:"minioAddr" desc:"override internal minio address"`
	SkipBucketCheck     bool   `name:"skipBucketCheck" default:"false" desc:"skip bucket existence check"`
	JSONOutput          bool   `name:"json" default:"false" desc:"output structured JSON report"`
	Top                 int64  `name:"top" default:"10" desc:"number of largest footers to display"`
}

type parquetFooterFile struct {
	SegmentID     int64   `json:"segment_id"`
	Path          string  `json:"path"`
	Backend       string  `json:"backend"`
	FileBytes     int64   `json:"file_bytes"`
	MetadataBytes int64   `json:"metadata_bytes"`
	FooterBytes   int64   `json:"footer_bytes"`
	FooterPercent float64 `json:"footer_percent"`
	Error         string  `json:"error,omitempty"`
}
type parquetFooterSummary struct {
	Files         int     `json:"files"`
	FileBytes     int64   `json:"file_bytes"`
	MetadataBytes int64   `json:"metadata_bytes"`
	FooterBytes   int64   `json:"footer_bytes"`
	FooterPercent float64 `json:"footer_percent"`
	Min           int64   `json:"min_footer_bytes"`
	Max           int64   `json:"max_footer_bytes"`
	Mean          float64 `json:"mean_footer_bytes"`
	P50           int64   `json:"p50_footer_bytes"`
	P95           int64   `json:"p95_footer_bytes"`
}
type parquetFooterReport struct {
	CollectionID    int64                `json:"collection_id"`
	Segments        int                  `json:"segments"`
	SkippedSegments []int64              `json:"skipped_segments"`
	Errors          []string             `json:"errors"`
	Files           []parquetFooterFile  `json:"files"`
	Summary         parquetFooterSummary `json:"summary"`
}

// Parquet ends with serialized FileMetaData, a little-endian uint32 length,
// and PAR1. FooterBytes includes the eight-byte trailer, MetadataBytes does not.
func readParquetFooterSize(ctx context.Context, reader v3RangeReader, key string, size int64) (int64, error) {
	if size < 12 {
		return 0, fmt.Errorf("file too small for parquet: %d", size)
	}
	header, err := reader.ReadRange(ctx, key, 0, 4)
	if err != nil {
		return 0, err
	}
	if string(header) != "PAR1" {
		return 0, fmt.Errorf("not a plain parquet file (header %q)", header)
	}
	trailer, err := reader.ReadRange(ctx, key, size-8, 8)
	if err != nil {
		return 0, err
	}
	if len(trailer) != 8 {
		return 0, fmt.Errorf("invalid trailer length: %d", len(trailer))
	}
	if string(trailer[4:]) != "PAR1" {
		return 0, fmt.Errorf("unsupported parquet trailer magic %q", trailer[4:])
	}
	n := int64(binary.LittleEndian.Uint32(trailer[:4]))
	if n == 0 || n > size-12 {
		return 0, fmt.Errorf("invalid parquet metadata length %d for file size %d", n, size)
	}
	return n, nil
}

func summarizeParquetFooters(files []parquetFooterFile) parquetFooterSummary {
	var s parquetFooterSummary
	var sizes []int64
	for _, f := range files {
		if f.Error != "" {
			continue
		}
		s.Files++
		s.FileBytes += f.FileBytes
		s.MetadataBytes += f.MetadataBytes
		s.FooterBytes += f.FooterBytes
		sizes = append(sizes, f.FooterBytes)
	}
	if s.Files == 0 {
		return s
	}
	sort.Slice(sizes, func(i, j int) bool { return sizes[i] < sizes[j] })
	s.Min = sizes[0]
	s.Max = sizes[len(sizes)-1]
	s.Mean = float64(s.FooterBytes) / float64(s.Files)
	s.P50 = sizes[(len(sizes)*50+99)/100-1]
	s.P95 = sizes[(len(sizes)*95+99)/100-1]
	s.FooterPercent = 100 * float64(s.FooterBytes) / float64(s.FileBytes)
	return s
}

func (s *InstanceState) ShowParquetFooterCommand(ctx context.Context, p *ShowParquetFooterParam) error {
	if p.CollectionID <= 0 || p.SegmentID < 0 || p.Top < 0 {
		return fmt.Errorf("--collection must be positive; --segment and --top must be non-negative")
	}
	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(seg *models.Segment) bool {
		return seg.CollectionID == p.CollectionID && (p.SegmentID == 0 || seg.ID == p.SegmentID) && seg.State != commonpb.SegmentState_Dropped
	})
	if err != nil {
		return err
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].ID < segments[j].ID })
	internal, err := s.getObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, p.SkipBucketCheck)
	if err != nil {
		return err
	}
	collection, err := common.GetCollectionByIDVersion(ctx, s.client, s.basePath, p.CollectionID)
	if err != nil {
		return err
	}
	var external oss.ObjectStore
	var location ossutil.ExternalSourceLocation
	if collection.GetProto().GetSchema().GetExternalSource() != "" {
		resolved, loc, e := ossutil.NewResolvedExternalObjectStoreFromCollection(ctx, collection, p.SkipBucketCheck)
		if e != nil {
			return e
		}
		external = resolved.Store
		location = loc
	}
	report := parquetFooterReport{CollectionID: p.CollectionID, Segments: len(segments), Files: []parquetFooterFile{}, Errors: []string{}, SkippedSegments: []int64{}}
	for _, seg := range segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		if seg.GetManifestPath() == "" {
			if seg.GetStorageVersion() != 2 {
				report.SkippedSegments = append(report.SkippedSegments, seg.ID)
				continue
			}
			seen := map[string]bool{}
			for _, field := range seg.GetBinlogs() {
				for _, log := range field.Binlogs {
					key := oss.ResolveObjectKey(internal.RootPath, log.LogPath)
					if seen[key] {
						continue
					}
					seen[key] = true
					report.Files = append(report.Files, inspectParquetFooter(ctx, internal.Store, key, "internal", seg.ID))
				}
			}
			continue
		}
		m, base, e := loadFooterManifest(ctx, internal, seg.GetManifestPath())
		if e != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("segment %d: %v", seg.ID, e))
			continue
		}
		resolver := ossutil.NewManifestPathResolver(internal.Store, base, external, location)
		seen := map[string]bool{}
		for _, cg := range m.ColumnGroups {
			if cg.Format != "" && !strings.EqualFold(cg.Format, "parquet") {
				report.Errors = append(report.Errors, fmt.Sprintf("segment %d: unsupported column group format %q", seg.ID, cg.Format))
				continue
			}
			for _, f := range cg.Files {
				store, key, backend, e := resolver.Resolve(f.Path, "_data")
				if e != nil {
					report.Errors = append(report.Errors, fmt.Sprintf("segment %d file %s: %v", seg.ID, f.Path, e))
					continue
				}
				identity := fmt.Sprintf("%s:%s", backend, key)
				if seen[identity] {
					continue
				}
				seen[identity] = true
				report.Files = append(report.Files, inspectParquetFooter(ctx, store, key, fmt.Sprint(backend), seg.ID))
			}
		}
	}
	report.Summary = summarizeParquetFooters(report.Files)
	if p.JSONOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	printParquetFooterReport(report, p.Top)
	return nil
}

func loadFooterManifest(ctx context.Context, store *oss.ResolvedObjectStore, raw string) (*manifest, string, error) {
	var ref struct {
		Ver      int    `json:"ver"`
		BasePath string `json:"base_path"`
	}
	if err := json.Unmarshal([]byte(raw), &ref); err != nil {
		return nil, "", err
	}
	if ref.BasePath == "" || ref.Ver < 0 {
		return nil, "", fmt.Errorf("invalid manifest reference")
	}
	base := oss.ResolveObjectKey(store.RootPath, ref.BasePath)
	obj, err := store.Store.Open(ctx, path.Join(base, "_metadata", fmt.Sprintf("manifest-%d.avro", ref.Ver)))
	if err != nil {
		return nil, "", err
	}
	if closer, ok := obj.(io.Closer); ok {
		defer closer.Close()
	}
	m, err := parseManifest(obj)
	return m, base, err
}
func inspectParquetFooter(ctx context.Context, store oss.ObjectStore, key, backend string, segment int64) parquetFooterFile {
	f := parquetFooterFile{SegmentID: segment, Path: key, Backend: backend}
	if store == nil {
		f.Error = "no object store configured for file backend"
		return f
	}
	stat, err := store.Stat(ctx, key)
	if err != nil {
		f.Error = err.Error()
		return f
	}
	f.FileBytes = stat.Size
	n, err := readParquetFooterSize(ctx, objectStoreV3RangeReader{store: store}, key, stat.Size)
	if err != nil {
		f.Error = err.Error()
		return f
	}
	f.MetadataBytes = n
	f.FooterBytes = n + 8
	f.FooterPercent = 100 * float64(f.FooterBytes) / float64(f.FileBytes)
	return f
}
func printParquetFooterReport(r parquetFooterReport, top int64) {
	s := r.Summary
	fmt.Printf("Collection %d: segments=%d skipped=%d files=%d failed=%d manifest/resolve errors=%d\n", r.CollectionID, r.Segments, len(r.SkippedSegments), s.Files, len(r.Files)-s.Files, len(r.Errors))
	fmt.Println("Footer = serialized metadata + 8-byte trailer; totals count unique files per segment (shared files across segments count again).")
	fmt.Printf("File bytes=%d metadata bytes=%d footer bytes=%d footer/file=%.4f%%\n", s.FileBytes, s.MetadataBytes, s.FooterBytes, s.FooterPercent)
	fmt.Printf("Footer bytes: min=%d mean=%.2f p50=%d p95=%d max=%d (nearest-rank percentiles)\n", s.Min, s.Mean, s.P50, s.P95, s.Max)
	for _, id := range r.SkippedSegments {
		fmt.Printf("SKIP segment %d: no manifest and storage version is not 2\n", id)
	}
	for _, e := range r.Errors {
		fmt.Printf("ERROR %s\n", e)
	}
	bySegment := map[int64][]parquetFooterFile{}
	var ranked []parquetFooterFile
	for _, f := range r.Files {
		bySegment[f.SegmentID] = append(bySegment[f.SegmentID], f)
		if f.Error != "" {
			fmt.Printf("ERROR segment %d %s: %s\n", f.SegmentID, f.Path, f.Error)
		} else {
			ranked = append(ranked, f)
		}
	}
	var ids []int64
	for id := range bySegment {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		summary := summarizeParquetFooters(bySegment[id])
		fmt.Printf("Segment %d: files=%d footer bytes=%d footer/file=%.4f%%\n", id, summary.Files, summary.FooterBytes, summary.FooterPercent)
	}
	sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].FooterBytes > ranked[j].FooterBytes })
	fmt.Printf("Largest footers (top %d):\n", top)
	for i, f := range ranked {
		if int64(i) >= top {
			break
		}
		fmt.Printf("  segment=%d footer=%d metadata=%d file=%d ratio=%.4f%% backend=%s %s\n", f.SegmentID, f.FooterBytes, f.MetadataBytes, f.FileBytes, f.FooterPercent, f.Backend, f.Path)
	}
}
