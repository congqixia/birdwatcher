package storage

import (
	"fmt"
	"io"
	"os"

	"github.com/milvus-io/birdwatcher/proto/v2.0/schemapb"
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
	fmt.Println(header)
	ifed, err := readIndexFileEventData(f)
	if err != nil {
		return nil, err
	}
	fmt.Println(ifed)

	switch dataType {
	case schemapb.DataType_String:

	}
	next := header.EventLength - header.GetMemoryUsageInBytes() - ifed.GetEventDataFixPartSize()
	data := make([]byte, next)
	io.ReadFull(f, data)

	pr, err := NewParquetPayloadReader(dataType, data)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	result, err := pr.GetBytesFromPayload()
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	return result, nil
}

/*
func readerIndexEventData(buffer io.Reader) (any, error) {
	pr, err := NewParquetPayloadReader(schemapb.DataType_Int64, data)
	if err != nil {
		return nil, err
	}

}*/
