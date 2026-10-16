package states

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
)

const maxParquetHeaderBytes int64 = 16 * 1024 * 1024

// Header reads grow only when required. The payload is never decoded. A page
// scanner wraps this with read-ahead caching to coalesce nearby page headers.
func readCompactHeader(ctx context.Context, reader v3RangeReader, key string, offset, end int64) (*compactNode, error) {
	if offset < 4 || offset >= end {
		return nil, fmt.Errorf("header offset %d outside [%d,%d)", offset, 4, end)
	}
	for window := int64(256); ; window *= 2 {
		size := window
		if size > end-offset {
			size = end - offset
		}
		if size > maxParquetHeaderBytes {
			size = maxParquetHeaderBytes
		}
		data, err := reader.ReadRange(ctx, key, offset, size)
		if err != nil {
			return nil, err
		}
		node, err := scanCompactStruct(data)
		if err == nil {
			return node, nil
		}
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, err
		}
		if size == end-offset {
			return nil, fmt.Errorf("truncated compact header at %d", offset)
		}
		if size == maxParquetHeaderBytes {
			return nil, fmt.Errorf("compact header exceeds %d bytes", maxParquetHeaderBytes)
		}
	}
}

type parquetHeaderCache struct {
	reader     v3RangeReader
	key        string
	end, start int64
	data       []byte
}

func (c *parquetHeaderCache) ReadRange(ctx context.Context, key string, offset, size int64) ([]byte, error) {
	if key != c.key || offset < 4 || size <= 0 || offset > c.end || size > c.end-offset {
		return nil, fmt.Errorf("invalid page header range")
	}
	if offset >= c.start && offset-c.start <= int64(len(c.data)) && size <= int64(len(c.data))-(offset-c.start) {
		return c.data[offset-c.start : offset-c.start+size], nil
	}
	window := int64(64 * 1024)
	if size > window {
		window = size
	}
	if window > c.end-offset {
		window = c.end - offset
	}
	data, err := c.reader.ReadRange(ctx, key, offset, window)
	if err != nil {
		return nil, err
	}
	c.start = offset
	c.data = data
	return data[:size], nil
}
func analyzeParquetPages(ctx context.Context, reader v3RangeReader, key string, start, end int64) ([]parquetPageSize, error) {
	cache := &parquetHeaderCache{reader: reader, key: key, end: end}
	totals := map[string]parquetPageSize{}
	for offset := start; offset < end; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		header, err := readCompactHeader(ctx, cache, key, offset, end)
		if err != nil {
			return nil, err
		}
		typ, hasType := compactInt(header, 1)
		uncompressed, hasUncompressed := compactInt(header, 2)
		compressed, hasCompressed := compactInt(header, 3)
		if !hasType || !hasCompressed || !hasUncompressed || compressed < 0 || uncompressed < 0 {
			return nil, fmt.Errorf("invalid page header at %d", offset)
		}
		// Parquet page sizes are signed i32, regardless of generic compact integer width.
		if compressed > 1<<31-1 || uncompressed > 1<<31-1 {
			return nil, fmt.Errorf("page size overflows i32 at %d", offset)
		}
		headerBytes := int64(header.end)
		if compressed > end-offset-headerBytes {
			return nil, fmt.Errorf("page at %d exceeds column chunk end %d", offset, end)
		}
		name := fmt.Sprintf("UNKNOWN_%d", typ)
		switch typ {
		case 0:
			name = "DATA_PAGE"
		case 1:
			name = "INDEX_PAGE"
		case 2:
			name = "DICTIONARY_PAGE"
		case 3:
			name = "DATA_PAGE_V2"
		}
		p := totals[name]
		p.Type = name
		p.Count++
		p.HeaderBytes += headerBytes
		p.CompressedPayloadBytes += compressed
		p.UncompressedPayloadBytes += uncompressed
		totals[name] = p
		offset += headerBytes + compressed
	}
	var pages []parquetPageSize
	for _, p := range totals {
		pages = append(pages, p)
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i].Type < pages[j].Type })
	return pages, nil
}
func mergeParquetPages(a, b []parquetPageSize) []parquetPageSize {
	for _, page := range b {
		found := false
		for i := range a {
			if a[i].Type == page.Type {
				a[i].Count += page.Count
				a[i].HeaderBytes += page.HeaderBytes
				a[i].CompressedPayloadBytes += page.CompressedPayloadBytes
				a[i].UncompressedPayloadBytes += page.UncompressedPayloadBytes
				found = true
				break
			}
		}
		if !found {
			a = append(a, page)
		}
	}
	sort.Slice(a, func(i, j int) bool { return a[i].Type < a[j].Type })
	return a
}
