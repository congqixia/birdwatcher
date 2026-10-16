package states

import (
	"context"
	"fmt"
	"sort"

	"github.com/apache/arrow/go/v17/parquet/metadata"
)

type parquetSizePart struct {
	Name    string  `json:"name"`
	Bytes   int64   `json:"bytes"`
	Percent float64 `json:"percent"`
}
type parquetKVSize struct {
	Key             string `json:"key"`
	ValueBytes      int64  `json:"value_bytes"`
	SerializedBytes int64  `json:"serialized_bytes"`
}
type parquetPageSize struct {
	Type                     string `json:"type"`
	Count                    int64  `json:"count"`
	HeaderBytes              int64  `json:"header_bytes"`
	CompressedPayloadBytes   int64  `json:"compressed_payload_bytes"`
	UncompressedPayloadBytes int64  `json:"uncompressed_payload_bytes"`
}
type parquetColumnSize struct {
	Column                   string            `json:"column"`
	PhysicalType             string            `json:"physical_type"`
	Codec                    string            `json:"codec"`
	Encodings                []string          `json:"encodings"`
	Values                   int64             `json:"values"`
	CompressedBytes          int64             `json:"compressed_bytes"`
	UncompressedEncodedBytes int64             `json:"uncompressed_encoded_bytes"`
	CompressionRatio         float64           `json:"compression_ratio"`
	FilePercent              float64           `json:"file_percent"`
	FooterBytes              int64             `json:"footer_bytes"`
	StatisticsBytes          int64             `json:"statistics_bytes"`
	MinMaxValueBytes         int64             `json:"min_max_value_bytes"`
	Pages                    []parquetPageSize `json:"pages,omitempty"`
}
type parquetRowGroupSize struct {
	Index           int                 `json:"index"`
	Rows            int64               `json:"rows"`
	CompressedBytes int64               `json:"compressed_bytes"`
	BytesPerRow     float64             `json:"bytes_per_row"`
	Columns         []parquetColumnSize `json:"columns"`
}
type parquetFileAnalysis struct {
	Path                      string                `json:"path"`
	Backend                   string                `json:"backend"`
	ManifestPaths             []string              `json:"manifest_paths,omitempty"`
	FileBytes                 int64                 `json:"file_bytes"`
	Rows                      int64                 `json:"rows"`
	PhysicalColumns           int                   `json:"physical_columns"`
	MetadataBytes             int64                 `json:"metadata_bytes"`
	FooterBytes               int64                 `json:"footer_bytes"`
	FooterPercent             float64               `json:"footer_percent"`
	FooterBytesPerRow         float64               `json:"footer_bytes_per_row"`
	FooterBytesPerRowGroup    float64               `json:"footer_bytes_per_row_group"`
	FooterBytesPerColumnChunk float64               `json:"footer_bytes_per_column_chunk"`
	Composition               []parquetSizePart     `json:"composition"`
	FooterComposition         []parquetSizePart     `json:"footer_composition"`
	KeyValueMetadata          []parquetKVSize       `json:"key_value_metadata"`
	Columns                   []parquetColumnSize   `json:"columns"`
	RowGroups                 []parquetRowGroupSize `json:"row_groups"`
	Findings                  []string              `json:"findings"`
	PagesAnalyzed             bool                  `json:"pages_analyzed"`
	Pages                     []parquetPageSize     `json:"pages,omitempty"`
	Error                     string                `json:"error,omitempty"`
}
type parquetInterval struct {
	start, end int64
	kind       string
}

func accountParquetIntervals(size int64, intervals []parquetInterval) ([]parquetSizePart, error) {
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].start < intervals[j].start })
	totals := map[string]int64{}
	var end int64
	for _, r := range intervals {
		if r.start < 0 || r.end < r.start || r.end > size {
			return nil, fmt.Errorf("invalid %s range [%d,%d) for file size %d", r.kind, r.start, r.end, size)
		}
		if r.start < end {
			return nil, fmt.Errorf("overlapping %s range [%d,%d), previous end %d", r.kind, r.start, r.end, end)
		}
		totals["unclassified"] += r.start - end
		totals[r.kind] += r.end - r.start
		end = r.end
	}
	totals["unclassified"] += size - end
	return parquetSizeParts(totals, size), nil
}
func parquetSizeParts(totals map[string]int64, total int64) []parquetSizePart {
	parts := make([]parquetSizePart, 0, len(totals))
	for k, v := range totals {
		p := parquetSizePart{Name: k, Bytes: v}
		if total > 0 {
			p.Percent = 100 * float64(v) / float64(total)
		}
		parts = append(parts, p)
	}
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].Bytes == parts[j].Bytes {
			return parts[i].Name < parts[j].Name
		}
		return parts[i].Bytes > parts[j].Bytes
	})
	return parts
}

// footerBreakdown partitions exact original bytes into mutually exclusive
// categories. Column statistics are a subset of per-column footer attribution,
// but a separate category in the disjoint footer composition.
func footerBreakdown(root *compactNode) (map[string]int64, []parquetKVSize, error) {
	parts := map[string]int64{"other": int64(root.end - root.start)}
	for _, entry := range []struct {
		id   int16
		name string
	}{{2, "schema"}, {5, "key_value_metadata"}} {
		bytes := compactFieldSize(root.field(entry.id))
		parts[entry.name] = bytes
		parts["other"] -= bytes
	}
	var kvs []parquetKVSize
	if kv := root.child(5); kv != nil {
		for _, item := range kv.items {
			key, value := item.child(1), item.child(2)
			if key == nil {
				return nil, nil, fmt.Errorf("key-value entry missing key")
			}
			entry := parquetKVSize{Key: string(key.binary), SerializedBytes: int64(item.end - item.start)}
			if value != nil {
				entry.ValueBytes = int64(len(value.binary))
			}
			kvs = append(kvs, entry)
		}
	}
	if field := root.field(4); field != nil {
		size := compactFieldSize(field)
		parts["other"] -= size
		parts["row_group_overhead"] = size
		for _, rg := range field.value.items {
			columns := rg.child(1)
			if columns == nil {
				return nil, nil, fmt.Errorf("row group missing columns")
			}
			for _, column := range columns.items {
				bytes := int64(column.end - column.start)
				stats := compactFieldSize(column.child(3).field(12))
				parts["row_group_overhead"] -= bytes
				parts["column_statistics"] += stats
				parts["column_metadata_other"] += bytes - stats
			}
		}
	}
	return parts, kvs, nil
}

func analyzeParquetObject(ctx context.Context, reader v3RangeReader, key string, size, maxFooter int64, pages bool) (result parquetFileAnalysis, err error) {
	result = parquetFileAnalysis{Path: key, FileBytes: size, PagesAnalyzed: pages, Findings: []string{}}
	// Arrow's metadata schema initialization assumes a structurally valid schema.
	// Treat malformed-file panics as per-file failures, preserving other results.
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("invalid parquet metadata: %v", recovered)
		}
	}()
	n, err := readParquetFooterSize(ctx, reader, key, size)
	if err != nil {
		return result, err
	}
	if n > maxFooter {
		return result, fmt.Errorf("metadata size %d exceeds --maxFooterBytes %d", n, maxFooter)
	}
	footerStart := size - 8 - n
	raw, err := reader.ReadRange(ctx, key, footerStart, n)
	if err != nil {
		return result, err
	}
	root, err := scanCompactStruct(raw)
	if err != nil {
		return result, fmt.Errorf("scan footer: %w", err)
	}
	if root.end != len(raw) {
		return result, fmt.Errorf("footer contains %d trailing bytes", len(raw)-root.end)
	}
	parts, kvs, err := footerBreakdown(root)
	if err != nil {
		return result, err
	}
	md, err := metadata.NewFileMetaData(raw, nil)
	if err != nil {
		return result, fmt.Errorf("decode footer: %w", err)
	}
	result.Rows = md.GetNumRows()
	if result.Rows < 0 {
		return result, fmt.Errorf("negative file row count")
	}
	result.PhysicalColumns = md.Schema.NumColumns()
	result.MetadataBytes = n
	result.FooterBytes = n + 8
	result.FooterPercent = 100 * float64(n+8) / float64(size)
	result.FooterComposition = parquetSizeParts(parts, n)
	result.KeyValueMetadata = kvs
	intervals := []parquetInterval{{0, 4, "file_header"}, {footerStart, size - 8, "footer_metadata"}, {size - 8, size, "footer_trailer"}}
	groupNodes := root.child(4)
	if groupNodes == nil || len(groupNodes.items) != len(md.GetRowGroups()) {
		return result, fmt.Errorf("invalid row group metadata")
	}
	aggregates := map[string]*parquetColumnSize{}
	chunkCount := 0
	for i := range md.GetRowGroups() {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		rg := md.RowGroup(i)
		r := parquetRowGroupSize{Index: i, Rows: rg.NumRows()}
		if r.Rows < 0 {
			return result, fmt.Errorf("negative row count in row group %d", i)
		}
		colNodes := groupNodes.items[i].child(1)
		if colNodes == nil || len(colNodes.items) != rg.NumColumns() {
			return result, fmt.Errorf("row group %d column count mismatch", i)
		}
		for j := 0; j < rg.NumColumns(); j++ {
			chunkCount++
			c, e := rg.ColumnChunk(j)
			if e != nil {
				return result, e
			}
			if c.FilePath() != "" {
				return result, fmt.Errorf("column %s references separate file %q; external column chunks are unsupported", c.PathInSchema().String(), c.FilePath())
			}
			if c.CryptoMetadata() != nil {
				return result, fmt.Errorf("encrypted column %s is unsupported", c.PathInSchema().String())
			}
			cn := colNodes.items[j]
			cm := cn.child(3)
			if cm == nil {
				return result, fmt.Errorf("column missing metadata")
			}
			col := parquetColumnSize{Column: c.PathInSchema().String(), PhysicalType: c.Type().String(), Codec: c.Compression().String(), Values: c.NumValues(), CompressedBytes: c.TotalCompressedSize(), UncompressedEncodedBytes: c.TotalUncompressedSize(), FooterBytes: int64(cn.end - cn.start), StatisticsBytes: compactFieldSize(cm.field(12))}
			if col.Values < 0 {
				return result, fmt.Errorf("negative value count for column %s", col.Column)
			}
			for _, enc := range c.Encodings() {
				col.Encodings = append(col.Encodings, enc.String())
			}
			if stats := cm.child(12); stats != nil {
				for _, id := range []int16{1, 2, 5, 6} {
					if v := stats.child(id); v != nil {
						col.MinMaxValueBytes += int64(len(v.binary))
					}
				}
			}
			start := c.DataPageOffset()
			if c.HasDictionaryPage() && c.DictionaryPageOffset() < start {
				start = c.DictionaryPageOffset()
			}
			if c.HasIndexPage() && c.IndexPageOffset() < start {
				start = c.IndexPageOffset()
			}
			if col.CompressedBytes < 0 || col.UncompressedEncodedBytes < 0 || (col.CompressedBytes > 0 && (start < 4 || start > footerStart || col.CompressedBytes > footerStart-start)) {
				return result, fmt.Errorf("invalid column chunk range for %s", col.Column)
			}
			end := start + col.CompressedBytes
			if col.CompressedBytes > 0 {
				intervals = append(intervals, parquetInterval{start, end, "column_chunks"})
			}
			for _, index := range []struct {
				offset, length int16
				name           string
			}{{4, 5, "offset_indexes"}, {6, 7, "column_indexes"}} {
				off, hasOff := compactInt(cn, index.offset)
				length, hasLen := compactInt(cn, index.length)
				if hasOff && (off < 4 || off > footerStart) {
					return result, fmt.Errorf("invalid %s offset", index.name)
				}
				if hasOff && hasLen {
					if off < 4 || length < 0 || off > footerStart || length > footerStart-off {
						return result, fmt.Errorf("invalid %s range", index.name)
					}
					if length > 0 {
						intervals = append(intervals, parquetInterval{off, off + length, index.name})
					}
				} else if hasOff || hasLen {
					result.Findings = append(result.Findings, fmt.Sprintf("row group %d column %s: incomplete %s location; bytes remain unclassified", i, col.Column, index.name))
				}
			}
			if off, has := compactInt(cm, 14); has {
				if off < 4 || off > footerStart {
					return result, fmt.Errorf("invalid bloom filter offset")
				}
				length, hasLen := compactInt(cm, 15)
				if hasLen {
					if off < 4 || length < 0 || off > footerStart || length > footerStart-off {
						return result, fmt.Errorf("invalid bloom filter range")
					}
					if length > 0 {
						intervals = append(intervals, parquetInterval{off, off + length, "bloom_filters"})
					}
				} else {
					// Old writers omit bloom_filter_length. The small header contains the
					// bitset byte count; parse it without loading the bitset itself.
					header, e := readCompactHeader(ctx, reader, key, off, footerStart)
					if e != nil {
						result.Findings = append(result.Findings, fmt.Sprintf("column %s: bloom filter length unavailable: %v", col.Column, e))
					} else {
						bits, ok := compactInt(header, 1)
						headerLen := int64(header.end)
						if !ok || bits < 0 || bits > footerStart-off-headerLen {
							return result, fmt.Errorf("invalid bloom filter header")
						}
						intervals = append(intervals, parquetInterval{off, off + headerLen + bits, "bloom_filters"})
					}
				}
			}
			if pages && col.CompressedBytes > 0 {
				col.Pages, e = analyzeParquetPages(ctx, reader, key, start, end)
				if e != nil {
					return result, fmt.Errorf("row group %d column %s pages: %w", i, col.Column, e)
				}
				result.Pages = mergeParquetPages(result.Pages, col.Pages)
				var pageUncompressed int64
				for _, p := range col.Pages {
					pageUncompressed += p.HeaderBytes + p.UncompressedPayloadBytes
				}
				if pageUncompressed != col.UncompressedEncodedBytes {
					result.Findings = append(result.Findings, fmt.Sprintf("row group %d column %s: page uncompressed total %d differs from metadata %d", i, col.Column, pageUncompressed, col.UncompressedEncodedBytes))
				}
			}
			setColumnRatios(&col, size)
			r.Columns = append(r.Columns, col)
			r.CompressedBytes += col.CompressedBytes
			aggregate := aggregates[col.Column]
			if aggregate == nil {
				v := col
				v.Values = 0
				v.CompressedBytes = 0
				v.UncompressedEncodedBytes = 0
				v.FooterBytes = 0
				v.StatisticsBytes = 0
				v.MinMaxValueBytes = 0
				v.Pages = nil
				aggregate = &v
				aggregates[col.Column] = aggregate
			}
			aggregate.Values += col.Values
			aggregate.CompressedBytes += col.CompressedBytes
			aggregate.UncompressedEncodedBytes += col.UncompressedEncodedBytes
			aggregate.FooterBytes += col.FooterBytes
			aggregate.StatisticsBytes += col.StatisticsBytes
			aggregate.MinMaxValueBytes += col.MinMaxValueBytes
			if aggregate.Codec != col.Codec {
				aggregate.Codec = "mixed"
			}
			for _, enc := range col.Encodings {
				found := false
				for _, existing := range aggregate.Encodings {
					if enc == existing {
						found = true
						break
					}
				}
				if !found {
					aggregate.Encodings = append(aggregate.Encodings, enc)
				}
			}
			aggregate.Pages = mergeParquetPages(aggregate.Pages, col.Pages)
		}
		if r.Rows > 0 {
			r.BytesPerRow = float64(r.CompressedBytes) / float64(r.Rows)
		}
		result.RowGroups = append(result.RowGroups, r)
	}
	result.Composition, err = accountParquetIntervals(size, intervals)
	if err != nil {
		return result, err
	}
	for _, c := range aggregates {
		setColumnRatios(c, size)
		result.Columns = append(result.Columns, *c)
	}
	sort.Slice(result.Columns, func(i, j int) bool {
		if result.Columns[i].CompressedBytes == result.Columns[j].CompressedBytes {
			return result.Columns[i].Column < result.Columns[j].Column
		}
		return result.Columns[i].CompressedBytes > result.Columns[j].CompressedBytes
	})
	if result.Rows > 0 {
		result.FooterBytesPerRow = float64(result.FooterBytes) / float64(result.Rows)
	}
	if len(result.RowGroups) > 0 {
		result.FooterBytesPerRowGroup = float64(result.FooterBytes) / float64(len(result.RowGroups))
	}
	if chunkCount > 0 {
		result.FooterBytesPerColumnChunk = float64(result.FooterBytes) / float64(chunkCount)
	}
	addParquetFindings(&result, parts)
	return result, nil
}
func setColumnRatios(c *parquetColumnSize, size int64) {
	if c.CompressedBytes > 0 {
		c.CompressionRatio = float64(c.UncompressedEncodedBytes) / float64(c.CompressedBytes)
	}
	if size > 0 {
		c.FilePercent = 100 * float64(c.CompressedBytes) / float64(size)
	}
}
func addParquetFindings(r *parquetFileAnalysis, parts map[string]int64) {
	if parts["column_statistics"] > r.MetadataBytes/2 {
		r.Findings = append(r.Findings, "Column statistics account for over half of footer metadata; inspect the columns with the largest min/max byte counts before evaluating statistics truncation or disabling statistics.")
	}
	if parts["key_value_metadata"] > r.MetadataBytes/2 {
		r.Findings = append(r.Findings, "Key-value metadata accounts for over half of footer metadata; inspect large keys for redundant writer metadata.")
	}
	if len(r.RowGroups) > 100 && r.Rows/int64(len(r.RowGroups)) < 1000 {
		r.Findings = append(r.Findings, "More than 100 row groups with fewer than 1000 rows per group on average; evaluate larger writer batches/row groups and the effect on pruning.")
	}
	for _, p := range r.Composition {
		if p.Name == "unclassified" && p.Bytes > 0 {
			r.Findings = append(r.Findings, fmt.Sprintf("%d bytes are unclassified (gaps, padding, or unsupported structures); no corruption is inferred from gaps alone.", p.Bytes))
		}
	}
	for _, c := range r.Columns {
		var dict, data int64
		for _, p := range c.Pages {
			if p.Type == "DICTIONARY_PAGE" {
				dict += p.HeaderBytes + p.CompressedPayloadBytes
			}
			if p.Type == "DATA_PAGE" || p.Type == "DATA_PAGE_V2" {
				data += p.HeaderBytes + p.CompressedPayloadBytes
			}
		}
		if dict > data && dict > 0 {
			r.Findings = append(r.Findings, fmt.Sprintf("Column %s dictionary pages exceed data pages in size; evaluate dictionary encoding with representative data and query workloads.", c.Column))
		}
	}
}
