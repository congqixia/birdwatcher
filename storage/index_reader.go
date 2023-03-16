package storage

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
	"github.com/samber/lo"
)

type IndexReader struct {
}

func NewIndexReader(f *os.File) (*IndexReader, descriptorEvent, error) {
	reader := &IndexReader{}
	var de descriptorEvent
	var err error

	_, err = readMagicNumber(f)
	if err != nil {
		return nil, de, err
	}

	de, err = ReadDescriptorEvent(f)
	if err != nil {
		return nil, de, err
	}
	return reader, de, err
}

func (reader *IndexReader) NextEventReader(f *os.File, dataType schemapb.DataType) ([][]byte, error) {
	eventReader := newEventReader()
	header, err := eventReader.readHeader(f)
	if err != nil {
		//return nil, err
		return nil, err
	}
	ifed, err := readIndexFileEventData(f)
	if err != nil {
		return nil, err
	}

	next := header.EventLength - header.GetMemoryUsageInBytes() - ifed.GetEventDataFixPartSize()
	data := make([]byte, next)
	io.ReadFull(f, data)

	pr, err := NewParquetPayloadReader(dataType, data)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	switch dataType {
	case schemapb.DataType_String:
		result, err := pr.GetStringFromPayload()
		if err != nil {
			fmt.Println(err.Error())
			return nil, err
		}
		return lo.Map(result, func(data string, _ int) []byte {
			return []byte(data)
		}), nil
	case schemapb.DataType_Int8:
		result, err := pr.GetBytesFromPayload()
		if err != nil {
			fmt.Println(err.Error())
			return nil, err
		}
		return [][]byte{result}, nil

	}
	return nil, errors.New("unexpected data type")
}

/*
func readerIndexEventData(buffer io.Reader) (any, error) {
	pr, err := NewParquetPayloadReader(schemapb.DataType_Int64, data)
	if err != nil {
		return nil, err
	}

}*/
