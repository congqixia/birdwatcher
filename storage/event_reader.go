package storage

import (
	"encoding/binary"
	"errors"
	"io"
)

// EventReader binlog event reader from Milvus
type EventReader struct {
}

func (reader *EventReader) readHeader(in io.Reader) (*eventHeader, error) {

	header, err := readEventHeader(in)
	if err != nil {
		return nil, err
	}
	return header, nil
}

func (reader *EventReader) readData(in io.Reader, eventType EventTypeCode) error {
	switch eventType {
	case InsertEventType:
		_, err := readInsertEventDataFixPart(in)
		return err
	default:
		return errors.New("not supported yet")
	}
}

func readInsertEventData(buffer io.Reader) (insertEventData, error) {
	data := insertEventData{}
	if err := binary.Read(buffer, commonEndian, &data); err != nil {
		return data, err
	}

	return data, nil
}

func readIndexFileEventData(buffer io.Reader) (indexFileEventData, error) {
	data := indexFileEventData{}
	if err := binary.Read(buffer, commonEndian, &data); err != nil {
		return data, err
	}
	return data, nil
}

func newEventReader() *EventReader {
	return &EventReader{}
}
