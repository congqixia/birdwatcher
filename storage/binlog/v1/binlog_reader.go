package binlogv1

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/storage/common"
)

const (
	// MagicNumberV1 used in binlog
	MagicNumberV1 int32 = 0xfffabc
)

type DescriptorEvent struct {
	descriptorEventHeader
	descriptorEventData
}

// ReadDescriptorEvent reads a descriptorEvent from buffer
func ReadDescriptorEvent(buffer common.ReadSeeker) (DescriptorEvent, error) {
	de := DescriptorEvent{}
	header, err := readDescriptorEventHeader(buffer)
	if err != nil {
		return de, err
	}
	data, err := readDescriptorEventData(buffer)
	if err != nil {
		return de, err
	}
	return DescriptorEvent{
		descriptorEventHeader: *header,
		descriptorEventData:   *data,
	}, nil
}

func ReadMagicNumber(buffer common.ReadSeeker) (int32, error) {
	var magicNumber int32
	if err := binary.Read(buffer, commonEndian, &magicNumber); err != nil {
		return -1, err
	}
	if magicNumber != MagicNumberV1 {
		return -1, fmt.Errorf("parse magic number failed, expected: %d, actual: %d", MagicNumberV1, magicNumber)
	}

	return magicNumber, nil
}

// BinlogReader from Milvus.
type BinlogReader struct {
	reader io.Reader
	DescriptorEvent
}

func NewBinlogReader(f common.ReadSeeker) (*BinlogReader, error) {
	reader := &BinlogReader{
		reader: f,
	}
	var err error

	if _, err = reader.readMagicNumber(f); err != nil {
		return nil, err
	}

	if reader.DescriptorEvent, err = reader.readDescriptorEvent(f); err != nil {
		return nil, err
	}

	return reader, nil
}

// NextInt16EventReader returns next reader for the events.
func (reader *BinlogReader) NextBoolEventReader() ([]bool, error) {
	return readData[bool, *file.BooleanColumnChunkReader](reader.reader, 0)
}

// NextInt16EventReader returns next reader for the events.
func (reader *BinlogReader) NextInt8EventReader() ([]int8, error) {
	return readDataTrans[int32, int8, *file.Int32ColumnChunkReader](reader.reader, 0, func(v int32) int8 { return int8(v) })
}

// NextInt16EventReader returns next reader for the events.
func (reader *BinlogReader) NextInt16EventReader() ([]int16, error) {
	return readDataTrans[int32, int16, *file.Int32ColumnChunkReader](reader.reader, 0, func(v int32) int16 { return int16(v) })
}

// NextInt32EventReader returns next reader for the events.
func (reader *BinlogReader) NextInt32EventReader() ([]int32, error) {
	return readData[int32, *file.Int32ColumnChunkReader](reader.reader, 0)
}

// NextInt64EventReader returns next reader for the events.
func (reader *BinlogReader) NextInt64EventReader() ([]int64, error) {
	return readData[int64, *file.Int64ColumnChunkReader](reader.reader, 0)
}

func (reader *BinlogReader) NextFloat32EventReader() ([]float32, error) {
	return readData[float32, *file.Float32ColumnChunkReader](reader.reader, 0)
}

func (reader *BinlogReader) NextFloat64EventReader() ([]float64, error) {
	return readData[float64, *file.Float64ColumnChunkReader](reader.reader, 0)
}

func (reader *BinlogReader) NextVarcharEventReader() ([]string, error) {
	return readDataTrans[parquet.ByteArray, string, *file.ByteArrayColumnChunkReader](reader.reader, 0, func(v parquet.ByteArray) string {
		return v.String()
	})
}

func (reader *BinlogReader) NextByteSliceEventReader() ([][]byte, error) {
	return readDataTrans[parquet.ByteArray, []byte, *file.ByteArrayColumnChunkReader](reader.reader, 0, func(v parquet.ByteArray) []byte {
		return v
	})
}

func (reader *BinlogReader) NextBinaryVectorEventReader() ([][]byte, error) {
	return readDataTrans[parquet.FixedLenByteArray, []byte, *file.FixedLenByteArrayColumnChunkReader](reader.reader, 0, func(v parquet.FixedLenByteArray) []byte {
		return v
	})
}

func (reader *BinlogReader) NextFloatVectorEventReader() ([][]float32, error) {
	return readDataTrans[parquet.FixedLenByteArray, []float32, *file.FixedLenByteArrayColumnChunkReader](reader.reader, 0, func(v parquet.FixedLenByteArray) []float32 {
		dim := v.Len() / 4
		vector := make([]float32, dim)
		copy(arrow.Float32Traits.CastToBytes(vector), v)
		return vector
	})
}

func (reader *BinlogReader) NextRecordReader(ctx context.Context) (pqarrow.RecordReader, error) {
	fileReader, err := reader.NextParquetReader()
	if err != nil {
		return nil, err
	}
	arrowReader, err := pqarrow.NewFileReader(fileReader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}

	return arrowReader.GetRecordReader(ctx, nil, nil)
}

// NextParquetReader returns the underlying parquet file reader for the next insert event payload.
// Useful for inspecting parquet metadata without decoding the records.
func (reader *BinlogReader) NextParquetReader() (*file.Reader, error) {
	eventReader := NewEventReader()
	header, err := eventReader.ReadHeader(reader.reader)
	if err != nil {
		return nil, err
	}
	insertEventData, err := readInsertEventData(reader.reader)
	if err != nil {
		return nil, err
	}

	next := int(header.EventLength - header.GetMemoryUsageInBytes() - insertEventData.GetEventDataFixPartSize())

	data := make([]byte, next)
	if _, err := io.ReadFull(reader.reader, data); err != nil {
		return nil, err
	}

	return file.NewParquetReader(bytes.NewReader(data))
}

func (reader *BinlogReader) GetMapping() map[int64]int {
	return map[int64]int{reader.DescriptorEvent.FieldID: 0}
}

func (reader *BinlogReader) SelectFields(_ []int64) {}

func (reader *BinlogReader) readMagicNumber(f common.ReadSeeker) (int32, error) {
	var err error
	var magicNumber int32
	magicNumber, err = ReadMagicNumber(f)

	return magicNumber, err
}

func (reader *BinlogReader) readDescriptorEvent(f common.ReadSeeker) (DescriptorEvent, error) {
	event, err := ReadDescriptorEvent(f)
	if err != nil {
		return event, err
	}
	return event, nil
}

// Close closes the BinlogReader object.
// It mainly calls the Close method of the internal events, reclaims resources, and marks itself as closed.
func (reader *BinlogReader) Close() {
}

func readDataTrans[T any, U any, Reader interface {
	file.ColumnChunkReader
	ReadBatch(int64, []T, []int16, []int16) (int64, int, error)
}](f io.Reader, colIdx int, trans func(T) U) ([]U, error) {
	data, err := readData[T, Reader](f, colIdx)
	if err != nil {
		return nil, err
	}
	return lo.Map(data, func(v T, _ int) U {
		return trans(v)
	}), nil
}

func readData[T any, Reader interface {
	file.ColumnChunkReader
	ReadBatch(int64, []T, []int16, []int16) (int64, int, error)
}](f io.Reader, colIdx int) ([]T, error) {
	eventReader := NewEventReader()
	header, err := eventReader.ReadHeader(f)
	if err != nil {
		return nil, err
	}
	insertEventData, err := readInsertEventData(f)
	if err != nil {
		return nil, err
	}

	next := int(header.EventLength - header.GetMemoryUsageInBytes() - insertEventData.GetEventDataFixPartSize())

	data := make([]byte, next)
	io.ReadFull(f, data)

	pqReader, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	return readPayloadAll[T, Reader](pqReader, colIdx)
}
