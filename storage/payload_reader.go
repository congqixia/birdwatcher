package storage

import (
	"bytes"
	"errors"

	"github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
	"github.com/samber/lo"
)

type ParquetPayloadReader struct {
	colType schemapb.DataType
	reader  *file.Reader
}

// NewParquetPayloadReader get a PayloadReader with parquet file reader
func NewParquetPayloadReader(colType schemapb.DataType, buf []byte) (*ParquetPayloadReader, error) {
	if len(buf) == 0 {
		return nil, errors.New("create PayloadReader with empty buf")
	}

	reader, err := file.NewParquetReader(bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	return &ParquetPayloadReader{reader: reader, colType: colType}, nil
}

func (r *ParquetPayloadReader) GetInt64sFromPayload() ([]int64, error) {
	if r.colType != schemapb.DataType_Int64 {
		return nil, errors.New("data type not matched")
	}

	// looks weird
	reader, ok := r.reader.RowGroup(0).Column(0).(*file.Int64ColumnChunkReader)
	if !ok {
		return nil, errors.New("parquet reader type not match")
	}
	numRows := r.reader.NumRows()

	values := make([]int64, numRows)
	total, valuesRead, err := reader.ReadBatch(numRows, values, nil, nil)
	if err != nil {
		return nil, err
	}

	if total != numRows || valuesRead != int(numRows) {
		return nil, errors.New("numRows not match")
	}
	return values, nil
}

func (r *ParquetPayloadReader) GetStringFromPayload() ([]string, error) {
	reader, ok := r.reader.RowGroup(0).Column(0).(*file.ByteArrayColumnChunkReader)
	if !ok {
		return nil, errors.New("parquet reader type not match")
	}

	numRows := r.reader.NumRows()
	values := make([]parquet.ByteArray, numRows)
	total, valuesRead, err := reader.ReadBatch(numRows, values, nil, nil)
	if err != nil {
		return nil, err
	}
	if total != numRows || valuesRead != int(numRows) {
		return nil, errors.New("numRows not match")
	}

	return lo.Map(values, func(ba parquet.ByteArray, _ int) string {
		return ba.String()
	}), nil
}

func (r *ParquetPayloadReader) GetBytesFromPayload() ([]byte, error) {
	reader, ok := r.reader.RowGroup(0).Column(0).(*file.Int32ColumnChunkReader)
	if !ok {
		return nil, errors.New("parquet reader type not match")
	}

	numRows := r.reader.NumRows()
	values := make([]int32, numRows)
	total, valuesRead, err := reader.ReadBatch(numRows, values, nil, nil)
	if err != nil {
		return nil, err
	}
	if total != numRows || valuesRead != int(numRows) {
		return nil, errors.New("numRows not match")
	}

	return lo.Map(values, func(ba int32, _ int) byte {
		return byte(ba)
	}), nil
}
