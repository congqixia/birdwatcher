package binlogv2

import (
	"context"
	"io"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/storage/common"
)

type BinlogReader struct {
	r         common.ReadSeeker
	arrReader *pqarrow.FileReader

	mapping        map[int64]int // field id to column index
	selectedColumn []int

	rgIndex int
}

func (reader *BinlogReader) NextRecordReader(ctx context.Context) (pqarrow.RecordReader, error) {
	if reader.rgIndex >= reader.arrReader.ParquetReader().NumRowGroups() {
		return nil, io.EOF
	}
	rr, err := reader.arrReader.GetRecordReader(ctx, reader.selectedColumn, []int{reader.rgIndex})
	reader.rgIndex++
	return rr, err
}

func (reader *BinlogReader) GetMapping() map[int64]int {
	return reader.mapping
}

func (reader *BinlogReader) SelectFields(fields []int64) {
	outputFields := make(map[int]int64)
	reader.selectedColumn = lo.FilterMap(fields, func(v int64, _ int) (int, bool) {
		colIdx, ok := reader.mapping[v]
		if ok {
			outputFields[colIdx] = v
		}
		return colIdx, ok
	})

	// need to update output record mapping
	// say original mapping is:
	// idx    0,  1,   2,   3,   4
	// field  0,  1, 100, 101, 102
	// after select fields 1, 100
	// the selected column is [1, 2]
	// so, the output record mapping is {1:0, 100:1}

	filteredMap := make(map[int64]int)
	for idx, outputCol := range reader.selectedColumn {
		filteredMap[outputFields[outputCol]] = idx
	}
	reader.mapping = filteredMap
}

func (reader *BinlogReader) Close() {
}

// NewBinlogReader creates a new instance of BinlogReader with the provided ReadSeeker.
// It initializes the BinlogReader to read from the specified source.
func NewBinlogReader(f common.ReadSeeker) (*BinlogReader, error) {
	pqReader, err := file.NewParquetReader(f)
	if err != nil {
		return nil, err
	}

	arrReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	schema, err := arrReader.Schema()
	if err != nil {
		return nil, err
	}

	mapping := make(map[int64]int)
	for idx, field := range schema.Fields() {
		v, ok := field.Metadata.GetValue("PARQUET:field_id")
		if !ok {
			return nil, errors.Newf("pq field schema does not contain stv2 field id metadata, schema: %s", schema.String())
		}
		fieldID, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, errors.Newf("pq field schema does not contain stv2 field id metadata, schema: %s", schema.String())
		}
		mapping[fieldID] = idx
	}

	return &BinlogReader{
		r:         f,
		arrReader: arrReader,
		mapping:   mapping,
	}, nil
}
