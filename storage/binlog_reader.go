package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/samber/lo"
)

const (
	// MagicNumber used in binlog
	MagicNumber int32 = 0xfffabc
)

type ReadSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

// BinlogReader from Milvus.
type BinlogReader struct {
	reader io.Reader
}

type DescriptorEvent struct {
	descriptorEventHeader
	descriptorEventData
}

func NewBinlogReader(f ReadSeeker) (*BinlogReader, DescriptorEvent, error) {
	reader := &BinlogReader{
		reader: f,
	}
	var de DescriptorEvent
	var err error

	if _, err = reader.readMagicNumber(f); err != nil {
		return nil, de, err
	}

	if de, err = reader.readDescriptorEvent(f); err != nil {
		return nil, de, err
	}

	return reader, de, nil
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

func (reader *BinlogReader) NextJSONEventReader() ([]string, error) {
	return readDataTrans[parquet.ByteArray, string, *file.ByteArrayColumnChunkReader](reader.reader, 0, func(v parquet.ByteArray) string {
		return string(v)
	})
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

func (reader *BinlogReader) readMagicNumber(f ReadSeeker) (int32, error) {
	var err error
	var magicNumber int32
	magicNumber, err = readMagicNumber(f)

	return magicNumber, err
}

func (reader *BinlogReader) readDescriptorEvent(f ReadSeeker) (DescriptorEvent, error) {
	event, err := ReadDescriptorEvent(f)
	if err != nil {
		return event, err
	}
	return event, nil
}

func readMagicNumber(buffer ReadSeeker) (int32, error) {
	var magicNumber int32
	if err := binary.Read(buffer, commonEndian, &magicNumber); err != nil {
		return -1, err
	}
	if magicNumber != MagicNumber {
		return -1, fmt.Errorf("parse magic number failed, expected: %d, actual: %d", MagicNumber, magicNumber)
	}

	return magicNumber, nil
}

// ReadDescriptorEvent reads a descriptorEvent from buffer
func ReadDescriptorEvent(buffer ReadSeeker) (DescriptorEvent, error) {
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
	eventReader := newEventReader()
	header, err := eventReader.readHeader(f)
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
