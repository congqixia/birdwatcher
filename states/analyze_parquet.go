package states

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/ossutil"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

type AnalyzeParquetParam struct {
	OSSOverrideParam
	framework.ParamBase `use:"analyze parquet" desc:"analyze segment parquet storage, footer attribution and optional page layout"`
	SegmentID           int64  `name:"segment" default:"0" desc:"segment ID (required)"`
	File                string `name:"file" default:"" desc:"exact manifest file path or resolved object key; omitted analyzes all segment data files"`
	Pages               bool   `name:"pages" default:"false" desc:"scan page headers and skip payloads (additional range requests)"`
	RowGroups           bool   `name:"rowGroups" default:"false" desc:"print per-row-group column details (always included in JSON)"`
	Top                 int64  `name:"top" default:"10" desc:"number of largest files, columns and metadata keys to display"`
	MaxFooterBytes      int64  `name:"maxFooterBytes" default:"134217728" desc:"maximum serialized footer metadata bytes per file (128 MiB default)"`
	MinioAddress        string `name:"minioAddr" default:"" desc:"override internal OSS address"`
	SkipBucketCheck     bool   `name:"skipBucketCheck" default:"false" desc:"skip bucket existence check"`
	JSONOutput          bool   `name:"json" default:"false" desc:"output all analysis results as JSON with raw byte counts"`
}
type parquetAnalysisTarget struct {
	store         oss.ObjectStore
	key, backend  string
	manifestPaths []string
}
type parquetAnalysisReport struct {
	SegmentID         int64                 `json:"segment_id"`
	CollectionID      int64                 `json:"collection_id"`
	Files             []parquetFileAnalysis `json:"files"`
	Errors            []string              `json:"errors"`
	Summary           parquetFooterSummary  `json:"summary"`
	Composition       []parquetSizePart     `json:"composition"`
	FooterComposition []parquetSizePart     `json:"footer_composition"`
	Pages             []parquetPageSize     `json:"pages,omitempty"`
}

func (s *InstanceState) AnalyzeParquetCommand(ctx context.Context, p *AnalyzeParquetParam) error {
	if p.SegmentID <= 0 || p.Top < 0 || p.MaxFooterBytes <= 0 {
		return fmt.Errorf("--segment and --maxFooterBytes must be positive; --top must be non-negative")
	}
	if _, err := p.OSSOverrideParam.connectParams(); err != nil {
		return err
	}
	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(seg *models.Segment) bool {
		return seg.ID == p.SegmentID && seg.State != commonpb.SegmentState_Dropped
	})
	if err != nil {
		return err
	}
	if len(segments) != 1 {
		return fmt.Errorf("expected one non-dropped segment %d, found %d", p.SegmentID, len(segments))
	}
	seg := segments[0]
	store, err := s.getObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, p.SkipBucketCheck)
	if err != nil {
		return err
	}
	targets, discoveryErrors, err := s.parquetAnalysisTargets(ctx, seg, store, p)
	if err != nil {
		return err
	}
	report := parquetAnalysisReport{SegmentID: seg.ID, CollectionID: seg.CollectionID, Files: []parquetFileAnalysis{}, Errors: discoveryErrors}
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return err
		}
		result := parquetFileAnalysis{Path: target.key}
		if target.store == nil {
			result.Error = "no object store configured for file backend"
		} else {
			stat, e := target.store.Stat(ctx, target.key)
			if e != nil {
				result.Error = e.Error()
			} else {
				result, e = analyzeParquetObject(ctx, objectStoreV3RangeReader{store: target.store}, target.key, stat.Size, p.MaxFooterBytes, p.Pages)
				if e != nil {
					result.Error = e.Error()
				}
			}
		}
		result.Backend = target.backend
		result.ManifestPaths = target.manifestPaths
		report.Files = append(report.Files, result)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	summarizeParquetAnalysis(&report)
	if p.JSONOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	printParquetAnalysis(report, p)
	return nil
}
func (s *InstanceState) parquetAnalysisTargets(ctx context.Context, seg *models.Segment, internal *oss.ResolvedObjectStore, p *AnalyzeParquetParam) ([]parquetAnalysisTarget, []string, error) {
	var targets []parquetAnalysisTarget
	var findings []string
	seen := map[string]int{}
	add := func(store oss.ObjectStore, key, backend, raw string) {
		if p.File != "" && p.File != raw && p.File != key {
			return
		}
		id := backend + ":" + key
		if idx, ok := seen[id]; ok {
			if raw != "" {
				targets[idx].manifestPaths = append(targets[idx].manifestPaths, raw)
			}
			return
		}
		seen[id] = len(targets)
		target := parquetAnalysisTarget{store: store, key: key, backend: backend}
		if raw != "" {
			target.manifestPaths = []string{raw}
		}
		targets = append(targets, target)
	}
	if seg.GetManifestPath() == "" {
		if seg.GetStorageVersion() != 2 {
			return nil, nil, fmt.Errorf("segment %d has no manifest and storage version %d is unsupported; only plain Parquet storage version 2 or manifest layouts are supported", seg.ID, seg.GetStorageVersion())
		}
		for _, field := range seg.GetBinlogs() {
			for _, log := range field.Binlogs {
				add(internal.Store, oss.ResolveObjectKey(internal.RootPath, log.LogPath), "internal", log.LogPath)
			}
		}
	} else {
		m, base, err := loadFooterManifest(ctx, internal, seg.GetManifestPath())
		if err != nil {
			return nil, nil, err
		}
		var external oss.ObjectStore
		var location ossutil.ExternalSourceLocation
		// Build external storage only if a selected file requires it. Internal-only
		// analysis remains usable when external credentials are unavailable.
		needExternal := false
		for _, cg := range m.ColumnGroups {
			for _, f := range cg.Files {
				if strings.Contains(f.Path, "://") || strings.Contains(f.Path, "ROOT_PATH") {
					if p.File == "" || p.File == f.Path {
						needExternal = true
					}
				}
			}
		}
		// A resolved external object key cannot be recognized before resolving the
		// collection location; parse it whenever external references are present.
		hasExternal := false
		for _, cg := range m.ColumnGroups {
			for _, f := range cg.Files {
				if strings.Contains(f.Path, "://") || strings.Contains(f.Path, "ROOT_PATH") {
					hasExternal = true
				}
			}
		}
		var externalErr error
		var collection *models.Collection
		if hasExternal {
			collection, externalErr = common.GetCollectionByIDVersion(ctx, s.client, s.basePath, seg.CollectionID)
			if externalErr == nil {
				location, externalErr = ossutil.ResolveExternalLocationFromCollection(collection)
			}
			if externalErr != nil {
				findings = append(findings, fmt.Sprintf("external location: %v", externalErr))
			}
		}
		resolver := ossutil.NewManifestPathResolver(internal.Store, base, nil, location)
		if p.File != "" && hasExternal && externalErr == nil {
			for _, cg := range m.ColumnGroups {
				for _, f := range cg.Files {
					_, key, backend, e := resolver.Resolve(f.Path, "_data")
					if e == nil && backend == ossutil.FileBackendExternal && key == p.File {
						needExternal = true
					}
				}
			}
		}
		if needExternal && externalErr == nil {
			resolved, loc, e := ossutil.NewResolvedExternalObjectStoreFromCollection(ctx, collection, p.SkipBucketCheck)
			if e != nil {
				externalErr = e
				findings = append(findings, fmt.Sprintf("external store: %v", e))
			} else {
				external = resolved.Store
				location = loc
			}
		}
		resolver = ossutil.NewManifestPathResolver(internal.Store, base, external, location)
		for _, cg := range m.ColumnGroups {
			if cg.Format != "" && !strings.EqualFold(cg.Format, "parquet") {
				findings = append(findings, fmt.Sprintf("unsupported column group format %q", cg.Format))
				continue
			}
			for _, f := range cg.Files {
				selectedStore, key, backend, e := resolver.Resolve(f.Path, "_data")
				if e != nil {
					findings = append(findings, fmt.Sprintf("resolve %s: %v", f.Path, e))
					continue
				}
				if backend == ossutil.FileBackendExternal && externalErr != nil {
					selectedStore = nil
				}
				add(selectedStore, key, fmt.Sprint(backend), f.Path)
			}
		}
	}
	if p.File != "" && len(targets) == 0 {
		return nil, nil, fmt.Errorf("file %q does not match any supported data file in segment %d", p.File, seg.ID)
	}
	if p.File != "" && len(targets) > 1 {
		return nil, nil, fmt.Errorf("file %q matches multiple storage backends; use its full manifest path", p.File)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].backend == targets[j].backend {
			return targets[i].key < targets[j].key
		}
		return targets[i].backend < targets[j].backend
	})
	return targets, findings, nil
}
func summarizeParquetAnalysis(r *parquetAnalysisReport) {
	r.Pages = nil
	var footers []parquetFooterFile
	composition := map[string]int64{}
	footerComposition := map[string]int64{}
	for _, f := range r.Files {
		if f.Error != "" {
			continue
		}
		footers = append(footers, parquetFooterFile{FileBytes: f.FileBytes, MetadataBytes: f.MetadataBytes, FooterBytes: f.FooterBytes})
		for _, part := range f.Composition {
			composition[part.Name] += part.Bytes
		}
		for _, part := range f.FooterComposition {
			footerComposition[part.Name] += part.Bytes
		}
		r.Pages = mergeParquetPages(r.Pages, f.Pages)
	}
	r.Summary = summarizeParquetFooters(footers)
	r.Composition = parquetSizeParts(composition, r.Summary.FileBytes)
	r.FooterComposition = parquetSizeParts(footerComposition, r.Summary.MetadataBytes)
}
func printParquetAnalysis(r parquetAnalysisReport, p *AnalyzeParquetParam) {
	s := r.Summary
	fmt.Printf("Segment %d (collection %d): files=%d succeeded=%d failed=%d discovery errors=%d\n", r.SegmentID, r.CollectionID, len(r.Files), s.Files, len(r.Files)-s.Files, len(r.Errors))
	fmt.Println("Sizes describe serialized storage. Uncompressed encoded bytes and compression ratios do not measure in-memory usage. Footer includes the 8-byte trailer; footer composition excludes it.")
	fmt.Printf("Total file=%s footer=%s (%.4f%%) footer p50=%s p95=%s max=%s\n", formatFooterSize(float64(s.FileBytes)), formatFooterSize(float64(s.FooterBytes)), s.FooterPercent, formatFooterSize(float64(s.P50)), formatFooterSize(float64(s.P95)), formatFooterSize(float64(s.Max)))
	printParquetParts("Segment storage composition", r.Composition)
	printParquetParts("Segment footer composition", r.FooterComposition)
	if p.Pages {
		printParquetPages("Segment pages (included in column chunks)", r.Pages)
	}
	for _, e := range r.Errors {
		fmt.Printf("ERROR %s\n", e)
	}
	var ranked []parquetFileAnalysis
	for _, f := range r.Files {
		if f.Error != "" {
			fmt.Printf("ERROR %s %s: %s\n", f.Backend, f.Path, f.Error)
			continue
		}
		ranked = append(ranked, f)
		fmt.Printf("\nFile: %s [%s]\nSize=%s rows=%d row groups=%d physical columns=%d footer=%s (%.4f%%)\n", f.Path, f.Backend, formatFooterSize(float64(f.FileBytes)), f.Rows, len(f.RowGroups), f.PhysicalColumns, formatFooterSize(float64(f.FooterBytes)), f.FooterPercent)
		fmt.Printf("Footer per row=%s per row group=%s per column chunk=%s\n", formatFooterSize(f.FooterBytesPerRow), formatFooterSize(f.FooterBytesPerRowGroup), formatFooterSize(f.FooterBytesPerColumnChunk))
		printParquetParts("Storage composition", f.Composition)
		printParquetParts("Footer composition", f.FooterComposition)
		if f.PagesAnalyzed {
			printParquetPages("File pages (included in column chunks)", f.Pages)
		}
		printParquetColumns("Largest columns by compressed size", f.Columns, p.Top, f.PagesAnalyzed)
		byFooter := append([]parquetColumnSize(nil), f.Columns...)
		sort.SliceStable(byFooter, func(i, j int) bool { return byFooter[i].FooterBytes > byFooter[j].FooterBytes })
		printParquetColumns("Largest columns by footer metadata", byFooter, p.Top, false)
		kvs := append([]parquetKVSize(nil), f.KeyValueMetadata...)
		sort.SliceStable(kvs, func(i, j int) bool { return kvs[i].SerializedBytes > kvs[j].SerializedBytes })
		if len(kvs) > 0 {
			fmt.Println("Largest file metadata keys (values are not printed):")
			for i, kv := range kvs {
				if int64(i) >= p.Top {
					break
				}
				fmt.Printf("  %q value=%s serialized=%s\n", kv.Key, formatFooterSize(float64(kv.ValueBytes)), formatFooterSize(float64(kv.SerializedBytes)))
			}
		}
		if p.RowGroups {
			for _, rg := range f.RowGroups {
				fmt.Printf("Row group %d: rows=%d compressed=%s per row=%s\n", rg.Index, rg.Rows, formatFooterSize(float64(rg.CompressedBytes)), formatFooterSize(rg.BytesPerRow))
				printParquetColumns("Columns", rg.Columns, int64(len(rg.Columns)), f.PagesAnalyzed)
			}
		}
		for _, finding := range f.Findings {
			fmt.Printf("Finding: %s\n", finding)
		}
	}
	for _, ranking := range []struct {
		name string
		less func(i, j int) bool
	}{
		{"footer size", func(i, j int) bool { return ranked[i].FooterBytes > ranked[j].FooterBytes }},
		{"footer percentage", func(i, j int) bool { return ranked[i].FooterPercent > ranked[j].FooterPercent }},
		{"file size", func(i, j int) bool { return ranked[i].FileBytes > ranked[j].FileBytes }},
	} {
		sort.SliceStable(ranked, ranking.less)
		fmt.Printf("\nTop files by %s:\n", ranking.name)
		for i, f := range ranked {
			if int64(i) >= p.Top {
				break
			}
			fmt.Printf("  %s footer=%s (%.4f%%) file=%s\n", f.Path, formatFooterSize(float64(f.FooterBytes)), f.FooterPercent, formatFooterSize(float64(f.FileBytes)))
		}
	}
}
func printParquetParts(label string, parts []parquetSizePart) {
	fmt.Printf("%s:\n", label)
	for _, part := range parts {
		fmt.Printf("  %-24s %12s %8.4f%%\n", part.Name, formatFooterSize(float64(part.Bytes)), part.Percent)
	}
}
func printParquetColumns(label string, cols []parquetColumnSize, top int64, pages bool) {
	fmt.Printf("%s:\n", label)
	for i, c := range cols {
		if int64(i) >= top {
			break
		}
		fmt.Printf("  %s (%s) codec=%s encodings=%v values=%d compressed=%s (%.4f%% of file) uncompressed encoded=%s ratio=%.2fx footer=%s stats=%s min/max values=%s\n", c.Column, c.PhysicalType, c.Codec, c.Encodings, c.Values, formatFooterSize(float64(c.CompressedBytes)), c.FilePercent, formatFooterSize(float64(c.UncompressedEncodedBytes)), c.CompressionRatio, formatFooterSize(float64(c.FooterBytes)), formatFooterSize(float64(c.StatisticsBytes)), formatFooterSize(float64(c.MinMaxValueBytes)))
		if pages {
			printParquetPages("  Pages", c.Pages)
		}
	}
}

func printParquetPages(label string, pages []parquetPageSize) {
	fmt.Printf("%s:\n", label)
	for _, pg := range pages {
		fmt.Printf("    %s count=%d headers=%s compressed payload=%s uncompressed payload=%s\n", pg.Type, pg.Count, formatFooterSize(float64(pg.HeaderBytes)), formatFooterSize(float64(pg.CompressedPayloadBytes)), formatFooterSize(float64(pg.UncompressedPayloadBytes)))
	}
}
