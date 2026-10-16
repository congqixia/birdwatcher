package states

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/metadata"
	"github.com/stretchr/testify/require"
)

func TestVerifyArrowInlineColumnMetadata(t *testing.T) {
	for _, pages := range []bool{false, true} {
		t.Run(fmt.Sprint(pages), func(t *testing.T) {
			data := makeInlineAnalysisParquet(t)
			baseline, err := analyzeParquetObject(context.Background(), &parquetTestRange{data: data}, "test", int64(len(data)), 1<<20, pages)
			require.NoError(t, err)
			var originalUnclassified int64
			for _, p := range baseline.Composition {
				if p.Name == "unclassified" {
					originalUnclassified = p.Bytes
				}
			}
			require.Positive(t, originalUnclassified, "fixture must contain flushed inline metadata copies")
			reader := &parquetTestRange{data: data}
			verified, err := analyzeParquetObjectWithLimits(context.Background(), reader, "test", int64(len(data)), 1<<20, pages, 0, true)
			require.NoError(t, err)
			v := verified.InlineMetadataVerification
			require.NotNil(t, v)
			require.Equal(t, 6, v.Candidates)
			require.Equal(t, 6, v.Matched)
			require.Zero(t, v.Unmatched)
			require.Zero(t, v.ReadErrors)
			require.Equal(t, originalUnclassified, v.OriginalUnclassifiedBytes)
			require.Equal(t, originalUnclassified, v.VerifiedBytes)
			require.Zero(t, v.RemainingUnclassifiedBytes)
			require.Equal(t, float64(100), v.VerifiedPercent)
			require.EqualValues(t, len(data), sumParquetParts(verified.Composition))
			require.Equal(t, baseline.FooterComposition, verified.FooterComposition)
			var inlineBytes, remaining int64
			for _, p := range verified.Composition {
				if p.Name == "inline_column_metadata" {
					inlineBytes = p.Bytes
				}
				if p.Name == "unclassified" {
					remaining = p.Bytes
				}
			}
			require.Equal(t, originalUnclassified, inlineBytes)
			require.Zero(t, remaining)
		})
	}
}
func inlineFixtureLocation(t *testing.T, data []byte) (int64, int) {
	t.Helper()
	n := int(binary.LittleEndian.Uint32(data[len(data)-8:]))
	raw := data[len(data)-8-n : len(data)-8]
	md, err := metadata.NewFileMetaData(raw, nil)
	require.NoError(t, err)
	root, err := scanCompactProjected(raw, compactFooter, 0)
	require.NoError(t, err)
	node := root.child(4).items[0].child(1).items[0]
	return md.GetRowGroups()[0].GetColumns()[0].GetFileOffset(), node.end - node.start
}
func TestInlineMetadataMismatchRemainsUnclassified(t *testing.T) {
	data := makeInlineAnalysisParquet(t)
	offset, length := inlineFixtureLocation(t, data)
	data = append([]byte(nil), data...)
	data[offset] ^= 1 // Only the inline copy, not the footer or pages.
	result, err := analyzeParquetObjectWithLimits(context.Background(), &parquetTestRange{data: data}, "test", int64(len(data)), 1<<20, false, 0, true)
	require.NoError(t, err)
	v := result.InlineMetadataVerification
	require.Equal(t, 5, v.Matched)
	require.Equal(t, 1, v.Unmatched)
	require.EqualValues(t, length, v.RemainingUnclassifiedBytes)
	require.Zero(t, v.ReadErrors)
	require.EqualValues(t, len(data), sumParquetParts(result.Composition))
}

type failingInlineReader struct {
	parquetTestRange
	offset int64
}

func (r *failingInlineReader) ReadRange(ctx context.Context, key string, offset, size int64) ([]byte, error) {
	if offset == r.offset {
		return nil, fmt.Errorf("inline range unavailable")
	}
	return r.parquetTestRange.ReadRange(ctx, key, offset, size)
}
func TestInlineMetadataReadFailureIsExplicit(t *testing.T) {
	data := makeInlineAnalysisParquet(t)
	offset, length := inlineFixtureLocation(t, data)
	reader := &failingInlineReader{parquetTestRange: parquetTestRange{data: data}, offset: offset}
	result, err := analyzeParquetObjectWithLimits(context.Background(), reader, "test", int64(len(data)), 1<<20, false, 0, true)
	require.NoError(t, err)
	v := result.InlineMetadataVerification
	require.Equal(t, 1, v.ReadErrors)
	require.Equal(t, 5, v.Matched)
	require.EqualValues(t, length, v.RemainingUnclassifiedBytes)
}
func TestVerifyInlineMetadataDoesNotReadOccupiedRanges(t *testing.T) {
	data := []byte("PAR1abcdefghijFOOTER")
	intervals := []parquetInterval{{0, 4, "header"}, {4, 10, "data"}, {14, int64(len(data)), "footer"}}
	candidates := []parquetInlineCandidate{{offset: 4, footerStart: 0, footerEnd: 3}, {offset: 10, footerStart: 0, footerEnd: 5}}
	reader := &parquetTestRange{data: data}
	_, v, err := verifyParquetInlineMetadata(context.Background(), reader, "test", int64(len(data)), []byte("12345"), intervals, candidates)
	require.NoError(t, err)
	require.Equal(t, 2, v.Unmatched)
	require.Zero(t, v.Matched)
	require.Empty(t, reader.reads)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = verifyParquetInlineMetadata(ctx, reader, "test", int64(len(data)), []byte("12345"), intervals, candidates)
	require.ErrorIs(t, err, context.Canceled)
}

// Rebuild a real Parquet with explicitly flushed ColumnChunk structs after each
// chunk. Some Go Arrow/Thrift combinations buffer those WriteTo bytes without
// flushing, so merely choosing an Arrow writer does not guarantee the layout.
func makeInlineAnalysisParquet(t *testing.T) []byte {
	t.Helper()
	data := makeAnalysisParquet(t, parquet.DataPageV2, true, compress.Codecs.Snappy)
	n := int(binary.LittleEndian.Uint32(data[len(data)-8:]))
	md, err := metadata.NewFileMetaData(data[len(data)-8-n:len(data)-8], nil)
	require.NoError(t, err)
	output := append([]byte(nil), data[:4]...)
	serializer := thrift.NewTSerializer()
	serializer.Protocol = thrift.NewTCompactProtocolFactoryConf(nil).GetProtocol(serializer.Transport)
	for _, rg := range md.GetRowGroups() {
		for _, cc := range rg.GetColumns() {
			cm := cc.MetaData
			start := cm.GetDataPageOffset()
			if cm.IsSetDictionaryPageOffset() && cm.GetDictionaryPageOffset() < start {
				start = cm.GetDictionaryPageOffset()
			}
			newStart := int64(len(output))
			delta := newStart - start
			output = append(output, data[start:start+cm.GetTotalCompressedSize()]...)
			cm.DataPageOffset += delta
			if cm.IsSetDictionaryPageOffset() {
				offset := cm.GetDictionaryPageOffset() + delta
				cm.DictionaryPageOffset = &offset
			}
			cc.FileOffset = int64(len(output))
			copyBytes, err := serializer.Write(context.Background(), cc)
			require.NoError(t, err)
			output = append(output, copyBytes...)
		}
	}
	raw, err := md.Serialize(context.Background())
	require.NoError(t, err)
	output = append(output, raw...)
	var trailer [8]byte
	binary.LittleEndian.PutUint32(trailer[:], uint32(len(raw)))
	copy(trailer[4:], "PAR1")
	output = append(output, trailer[:]...)
	return output
}
func TestInlineVerificationDoesNotInferFromWriterName(t *testing.T) {
	data := makeAnalysisParquet(t, parquet.DataPageV2, true, compress.Codecs.Snappy)
	result, err := analyzeParquetObjectWithLimits(context.Background(), &parquetTestRange{data: data}, "test", int64(len(data)), 1<<20, false, 0, true)
	require.NoError(t, err)
	// Whatever a particular Go Arrow version writes, only exact matching bytes
	// are classified. This must also hold when its writer leaves no gaps.
	v := result.InlineMetadataVerification
	require.Equal(t, v.OriginalUnclassifiedBytes, v.VerifiedBytes+v.RemainingUnclassifiedBytes)
	require.EqualValues(t, len(data), sumParquetParts(result.Composition))
}
