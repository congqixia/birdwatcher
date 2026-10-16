package states

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/metadata"
	"github.com/apache/arrow/go/v17/parquet/schema"
	"github.com/milvus-io/birdwatcher/configs"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

type parquetTestRange struct {
	data  []byte
	reads [][2]int64
}

func (r *parquetTestRange) ReadRange(ctx context.Context, _ string, offset, size int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 || size < 0 || offset > int64(len(r.data)) || size > int64(len(r.data))-offset {
		return nil, io.ErrUnexpectedEOF
	}
	r.reads = append(r.reads, [2]int64{offset, size})
	return r.data[offset : offset+size], nil
}
func makeAnalysisParquet(t *testing.T, version parquet.DataPageVersion, dict bool, codec compress.Compression) []byte {
	t.Helper()
	var buf bytes.Buffer
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{schema.NewInt32Node("id", parquet.Repetitions.Required, 100), schema.NewByteArrayNode("message", parquet.Repetitions.Required, 101)}, 0)
	require.NoError(t, err)
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(dict), parquet.WithCompression(codec), parquet.WithDataPageVersion(version), parquet.WithDataPageSize(256), parquet.WithBatchSize(64))
	writer := file.NewParquetWriter(&buf, root, file.WithWriterProps(props))
	require.NoError(t, writer.AppendKeyValueMetadata("large-key", strings.Repeat("metadata", 100)))
	for rg := 0; rg < 3; rg++ {
		rowGroup := writer.AppendRowGroup()
		cw, err := rowGroup.NextColumn()
		require.NoError(t, err)
		values := make([]int32, 512)
		for i := range values {
			values[i] = int32(i%13 + rg)
		}
		_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatch(values, nil, nil)
		require.NoError(t, err)
		require.NoError(t, cw.Close())
		cw, err = rowGroup.NextColumn()
		require.NoError(t, err)
		strings := make([]parquet.ByteArray, 512)
		for i := range strings {
			strings[i] = parquet.ByteArray(fmt.Sprintf("message-%04d", i%29))
		}
		_, err = cw.(*file.ByteArrayColumnChunkWriter).WriteBatch(strings, nil, nil)
		require.NoError(t, err)
		require.NoError(t, cw.Close())
		require.NoError(t, rowGroup.Close())
	}
	require.NoError(t, writer.Close())
	return buf.Bytes()
}
func sumParquetParts(parts []parquetSizePart) int64 {
	var sum int64
	for _, p := range parts {
		sum += p.Bytes
	}
	return sum
}
func TestAnalyzeParquetMetadataAndPages(t *testing.T) {
	for _, version := range []parquet.DataPageVersion{parquet.DataPageV1, parquet.DataPageV2} {
		for _, dict := range []bool{false, true} {
			for _, codec := range []compress.Compression{compress.Codecs.Uncompressed, compress.Codecs.Snappy} {
				t.Run(fmt.Sprintf("v%d/dict%v/%s", version, dict, codec), func(t *testing.T) {
					data := makeAnalysisParquet(t, version, dict, codec)
					reader := &parquetTestRange{data: data}
					result, err := analyzeParquetObject(context.Background(), reader, "test", int64(len(data)), 1<<20, false)
					require.NoError(t, err)
					require.EqualValues(t, 1536, result.Rows)
					require.Len(t, result.Columns, 2)
					require.Len(t, result.RowGroups, 3)
					require.EqualValues(t, len(data), sumParquetParts(result.Composition))
					require.Equal(t, result.MetadataBytes, sumParquetParts(result.FooterComposition))
					require.Len(t, result.KeyValueMetadata, 1)
					require.EqualValues(t, 800, result.KeyValueMetadata[0].ValueBytes)
					footerStart := int64(len(data)) - result.FooterBytes
					for _, r := range reader.reads {
						require.True(t, r[0] == 0 && r[1] == 4 || r[0] >= footerStart, "default mode read data pages: %v", r)
					}
					var columnFooters, stats int64
					for _, c := range result.Columns {
						columnFooters += c.FooterBytes
						stats += c.StatisticsBytes
						require.Greater(t, c.MinMaxValueBytes, int64(0))
					}
					var partitionColumns, partitionStats int64
					for _, part := range result.FooterComposition {
						if part.Name == "column_metadata_other" || part.Name == "column_statistics" {
							partitionColumns += part.Bytes
						}
						if part.Name == "column_statistics" {
							partitionStats = part.Bytes
						}
					}
					require.Equal(t, columnFooters, partitionColumns)
					require.Equal(t, stats, partitionStats)
					detailed, err := analyzeParquetObject(context.Background(), reader, "test", int64(len(data)), 1<<20, true)
					require.NoError(t, err)
					require.Equal(t, result.Composition, detailed.Composition)
					for _, rg := range detailed.RowGroups {
						for _, col := range rg.Columns {
							var compressed, uncompressed int64
							var hasDict, hasData bool
							for _, pg := range col.Pages {
								compressed += pg.HeaderBytes + pg.CompressedPayloadBytes
								uncompressed += pg.HeaderBytes + pg.UncompressedPayloadBytes
								if pg.Type == "DICTIONARY_PAGE" {
									hasDict = true
								}
								if pg.Type == "DATA_PAGE" || pg.Type == "DATA_PAGE_V2" {
									hasData = true
								}
							}
							require.Equal(t, col.CompressedBytes, compressed)
							require.Equal(t, col.UncompressedEncodedBytes, uncompressed)
							require.Equal(t, dict, hasDict)
							require.True(t, hasData)
						}
					}
				})
			}
		}
	}
}

func compactTestInt(v int64) []byte {
	var b [10]byte
	n := binary.PutUvarint(b[:], uint64(v<<1)^uint64(v>>63))
	return b[:n]
}
func compactTestField(id int16, kind byte, payload []byte) []byte {
	return append(append([]byte{kind}, compactTestInt(int64(id))...), payload...)
}
func compactTestPage(typ, compressed, uncompressed int64) []byte {
	var b []byte
	for i, v := range []int64{typ, uncompressed, compressed} {
		b = append(b, compactTestField(int16(i+1), 5, compactTestInt(v))...)
	}
	return append(b, 0)
}
func TestAnalyzeParquetPagesSkipsLargePayload(t *testing.T) {
	first := compactTestPage(2, 2<<20, 4<<20)
	second := compactTestPage(3, 128, 256)
	data := append([]byte("PAR1"), first...)
	data = append(data, make([]byte, 2<<20)...)
	data = append(data, second...)
	data = append(data, make([]byte, 128)...)
	reader := &parquetTestRange{data: data}
	pages, err := analyzeParquetPages(context.Background(), reader, "test", 4, int64(len(data)))
	require.NoError(t, err)
	require.Len(t, pages, 2)
	var readBytes int64
	for _, r := range reader.reads {
		readBytes += r[1]
	}
	require.Less(t, readBytes, int64(len(data)/10))
	require.Len(t, reader.reads, 2)
	var accounted int64
	for _, p := range pages {
		accounted += p.HeaderBytes + p.CompressedPayloadBytes
	}
	require.EqualValues(t, len(data)-4, accounted)
}
func TestAnalyzeParquetPageFailures(t *testing.T) {
	for _, header := range [][]byte{{0}, compactTestPage(0, -1, 10), compactTestPage(0, 100, 10), {0x15, 0x80}} {
		data := append([]byte("PAR1"), header...)
		_, err := analyzeParquetPages(context.Background(), &parquetTestRange{data: data}, "test", 4, int64(len(data)))
		require.Error(t, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := analyzeParquetPages(ctx, &parquetTestRange{}, "test", 4, 10)
	require.ErrorIs(t, err, context.Canceled)
}
func TestAccountParquetIntervals(t *testing.T) {
	parts, err := accountParquetIntervals(100, []parquetInterval{{0, 4, "header"}, {10, 20, "data"}, {90, 100, "footer"}})
	require.NoError(t, err)
	require.EqualValues(t, 100, sumParquetParts(parts))
	for _, ranges := range [][]parquetInterval{{{0, 10, "a"}, {5, 20, "b"}}, {{-1, 4, "bad"}}, {{0, 101, "bad"}}, {{10, 2, "bad"}}} {
		_, err := accountParquetIntervals(100, ranges)
		require.Error(t, err)
	}
}
func TestAnalyzeParquetFooterLimitAndMalformed(t *testing.T) {
	data := makeAnalysisParquet(t, parquet.DataPageV1, false, compress.Codecs.Uncompressed)
	_, err := analyzeParquetObject(context.Background(), &parquetTestRange{data: data}, "test", int64(len(data)), 1, false)
	require.ErrorContains(t, err, "maxFooterBytes")
	broken := append([]byte(nil), data...)
	binary.LittleEndian.PutUint32(broken[len(broken)-8:], uint32(len(broken)))
	_, err = analyzeParquetObject(context.Background(), &parquetTestRange{data: broken}, "test", int64(len(broken)), 1<<20, false)
	require.Error(t, err)
	n := int(binary.LittleEndian.Uint32(data[len(data)-8:]))
	broken = append([]byte(nil), data...)
	broken[len(broken)-8-n] = 0
	_, err = analyzeParquetObject(context.Background(), &parquetTestRange{data: broken}, "test", int64(len(broken)), 1<<20, false)
	require.Error(t, err)
}

func TestAnalyzeParquetIndexesBloomAndUnknownFields(t *testing.T) {
	original := makeAnalysisParquet(t, parquet.DataPageV1, false, compress.Codecs.Uncompressed)
	n := int(binary.LittleEndian.Uint32(original[len(original)-8:]))
	start := len(original) - 8 - n
	md, err := metadata.NewFileMetaData(original[start:len(original)-8], nil)
	require.NoError(t, err)
	cc := md.GetRowGroups()[0].GetColumns()[0]
	offsetIndex := int64(start)
	offsetLength := int32(3)
	columnIndex := offsetIndex + 3
	columnLength := int32(4)
	cc.OffsetIndexOffset = &offsetIndex
	cc.OffsetIndexLength = &offsetLength
	cc.ColumnIndexOffset = &columnIndex
	cc.ColumnIndexLength = &columnLength
	bloomOffset := columnIndex + 4
	cc.MetaData.BloomFilterOffset = &bloomOffset
	bloom := append(compactTestField(1, 5, compactTestInt(32)), 0)
	extra := append(make([]byte, 7), bloom...)
	extra = append(extra, make([]byte, 32)...)
	raw, err := md.Serialize(context.Background())
	require.NoError(t, err)
	// An unknown future field is attributed to "other", without reserialization.
	raw = append(raw[:len(raw)-1], compactTestField(100, 8, append([]byte{3}, []byte("new")...))...)
	raw = append(raw, 0)
	data := append(append([]byte(nil), original[:start]...), extra...)
	data = append(data, raw...)
	var trailer [8]byte
	binary.LittleEndian.PutUint32(trailer[:], uint32(len(raw)))
	copy(trailer[4:], "PAR1")
	data = append(data, trailer[:]...)
	result, err := analyzeParquetObject(context.Background(), &parquetTestRange{data: data}, "test", int64(len(data)), 1<<20, true)
	require.NoError(t, err)
	parts := map[string]int64{}
	for _, p := range result.Composition {
		parts[p.Name] = p.Bytes
	}
	require.EqualValues(t, 3, parts["offset_indexes"])
	require.EqualValues(t, 4, parts["column_indexes"])
	require.EqualValues(t, len(bloom)+32, parts["bloom_filters"])
	require.Zero(t, parts["unclassified"])
	require.EqualValues(t, len(raw), sumParquetParts(result.FooterComposition))
	require.EqualValues(t, len(data), sumParquetParts(result.Composition))
}

// Fuzz seeds also run under ordinary go test, covering malformed compact input.
func FuzzParquetCompact(f *testing.F) {
	for _, seed := range [][]byte{{0}, {0x15, 2, 0}, {0x19, 0xfc}, {0x18, 0x80}, {0x1c, 0, 0}, {0x1b, 0, 0}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<16 {
			t.Skip()
		}
		node, err := scanCompactStruct(data)
		if err == nil {
			require.LessOrEqual(t, node.end, len(data))
			require.Greater(t, node.end, 0)
		}
	})
}

type analysisMemoryStore struct{ objects map[string][]byte }

func (s *analysisMemoryStore) Open(_ context.Context, key string, _ ...oss.OpenOption) (storagecommon.ReadSeeker, error) {
	data, ok := s.objects[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return bytes.NewReader(data), nil
}
func (s *analysisMemoryStore) Stat(_ context.Context, key string) (*models.FsStat, error) {
	data, ok := s.objects[key]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return &models.FsStat{Size: int64(len(data))}, nil
}
func (s *analysisMemoryStore) List(context.Context, string, bool) (<-chan oss.ObjectInfo, error) {
	return nil, fmt.Errorf("unused")
}
func TestParquetAnalysisTargetSelection(t *testing.T) {
	// Legacy manifest v1: the same physical file appears in two column groups.
	var encoded manifestTestBinaryEncoder
	encoded.writeLong(int64(manifestMagic))
	encoded.writeLong(1)
	encoded.writeLong(2)
	for _, name := range []string{"a", "a"} {
		encoded.writeStringArray([]string{"100"})
		encoded.writeLong(1)
		encoded.writeString(name)
		encoded.writeLong(0)
		encoded.writeLong(10)
		encoded.writeBytes(nil)
		encoded.writeLong(0)
		encoded.writeString("parquet")
	}
	encoded.writeLong(0)
	encoded.writeLong(0)
	encoded.writeLong(0)
	internal := &oss.ResolvedObjectStore{Store: &analysisMemoryStore{objects: map[string][]byte{"base/_metadata/manifest-1.avro": encoded.Bytes()}}}
	seg := &models.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 7, ManifestPath: `{"ver":1,"base_path":"base"}`}}
	var state *InstanceState
	targets, findings, err := state.parquetAnalysisTargets(context.Background(), seg, internal, &AnalyzeParquetParam{})
	require.NoError(t, err)
	require.Empty(t, findings)
	require.Len(t, targets, 1)
	require.Equal(t, "base/_data/a", targets[0].key)
	for _, selected := range []string{"a", "base/_data/a"} {
		targets, _, err = state.parquetAnalysisTargets(context.Background(), seg, internal, &AnalyzeParquetParam{File: selected})
		require.NoError(t, err)
		require.Len(t, targets, 1)
	}
	_, _, err = state.parquetAnalysisTargets(context.Background(), seg, internal, &AnalyzeParquetParam{File: "missing"})
	require.ErrorContains(t, err, "does not match")
}

type analyzeFlagReceiver struct{ param *AnalyzeParquetParam }

func (r *analyzeFlagReceiver) AnalyzeCommand(_ context.Context, p *AnalyzeParquetParam) error {
	r.param = p
	return nil
}
func TestAnalyzeParquetFlags(t *testing.T) {
	host := &struct{ *framework.CmdState }{framework.NewCmdState("test", &configs.Config{})}
	receiver := &analyzeFlagReceiver{}
	root := &cobra.Command{SilenceUsage: true, SilenceErrors: true}
	host.MergeFunctionCommandsFrom(root, host, receiver)
	root.SetArgs([]string{"analyze", "parquet", "--segment", "7", "--pages", "--rowGroups", "--top", "5", "--maxFooterBytes", "1000", "--useIAM", "false"})
	require.NoError(t, root.Execute())
	require.NotNil(t, receiver.param)
	require.EqualValues(t, 7, receiver.param.SegmentID)
	require.True(t, receiver.param.Pages)
	require.True(t, receiver.param.RowGroups)
	require.EqualValues(t, 5, receiver.param.Top)
	require.EqualValues(t, 1000, receiver.param.MaxFooterBytes)
	require.Equal(t, "false", receiver.param.UseIAM)
}
func TestSummarizeParquetAnalysisExcludesFailures(t *testing.T) {
	r := parquetAnalysisReport{Files: []parquetFileAnalysis{{FileBytes: 100, MetadataBytes: 20, FooterBytes: 28, Composition: []parquetSizePart{{Name: "data", Bytes: 100}}, FooterComposition: []parquetSizePart{{Name: "schema", Bytes: 20}}}, {FileBytes: 200, MetadataBytes: 100, Error: "failure"}}}
	summarizeParquetAnalysis(&r)
	require.Equal(t, 1, r.Summary.Files)
	require.EqualValues(t, 100, r.Summary.FileBytes)
	require.EqualValues(t, 100, sumParquetParts(r.Composition))
	require.EqualValues(t, 20, sumParquetParts(r.FooterComposition))
}

func TestCompactHeaderGrowthAndValidation(t *testing.T) {
	header := compactTestPage(0, 0, 0)
	// Add a large unknown optional header field, forcing incremental range growth.
	var length [10]byte
	n := binary.PutUvarint(length[:], 4096)
	field := compactTestField(100, 8, append(length[:n], make([]byte, 4096)...))
	header = append(header[:len(header)-1], field...)
	header = append(header, 0)
	data := append([]byte("PAR1"), header...)
	reader := &parquetTestRange{data: data}
	node, err := readCompactHeader(context.Background(), reader, "test", 4, int64(len(data)))
	require.NoError(t, err)
	require.Equal(t, len(header), node.end)
	require.Greater(t, len(reader.reads), 1)
	duplicate := append(compactTestField(1, 5, compactTestInt(1)), compactTestField(1, 5, compactTestInt(2))...)
	duplicate = append(duplicate, 0)
	_, err = scanCompactStruct(duplicate)
	require.ErrorContains(t, err, "duplicate")
	// A boolean in a mandatory integer field must not be interpreted as a size.
	malformed := append([]byte{0x11}, compactTestField(2, 5, compactTestInt(0))...)
	malformed = append(malformed, compactTestField(3, 5, compactTestInt(0))...)
	malformed = append(malformed, 0)
	data = append([]byte("PAR1"), malformed...)
	_, err = analyzeParquetPages(context.Background(), &parquetTestRange{data: data}, "test", 4, int64(len(data)))
	require.Error(t, err)
}

func TestAnalyzeEmptyParquet(t *testing.T) {
	var buffer bytes.Buffer
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{schema.NewInt32Node("empty", parquet.Repetitions.Required, 1)}, 0)
	require.NoError(t, err)
	writer := file.NewParquetWriter(&buffer, root)
	require.NoError(t, writer.Close())
	data := buffer.Bytes()
	result, err := analyzeParquetObject(context.Background(), &parquetTestRange{data: data}, "test", int64(len(data)), 1<<20, true)
	require.NoError(t, err)
	require.Zero(t, result.Rows)
	require.Empty(t, result.RowGroups)
	require.EqualValues(t, len(data), sumParquetParts(result.Composition))
}
