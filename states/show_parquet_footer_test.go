package states

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
)

type footerTestReader struct {
	data      []byte
	bytesRead int
}

func (r *footerTestReader) ReadRange(_ context.Context, _ string, offset, size int64) ([]byte, error) {
	if offset < 0 || offset+size > int64(len(r.data)) {
		return nil, fmt.Errorf("bad range")
	}
	r.bytesRead += int(size)
	return r.data[offset : offset+size], nil
}
func TestReadParquetFooterSize(t *testing.T) {
	for _, tt := range []struct {
		name      string
		size      int
		length    uint32
		magic     string
		badHeader bool
		wantErr   bool
	}{
		{"valid", 1024, 100, "PAR1", false, false},
		{"too small", 8, 0, "PAR1", false, true},
		{"overflow", 32, 100, "PAR1", false, true},
		{"empty metadata", 32, 0, "PAR1", false, true},
		{"encrypted", 32, 10, "PARE", false, true},
		{"binlog", 32, 10, "PAR1", true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, tt.size)
			copy(data, "PAR1")
			binary.LittleEndian.PutUint32(data[len(data)-8:], tt.length)
			copy(data[len(data)-4:], tt.magic)
			if tt.badHeader {
				copy(data, "MILV")
			}
			reader := &footerTestReader{data: data}
			got, err := readParquetFooterSize(context.Background(), reader, "test", int64(len(data)))
			if (err != nil) != tt.wantErr {
				t.Fatalf("got %d, %v", got, err)
			}
			if !tt.wantErr && (got != int64(tt.length) || reader.bytesRead != 12) {
				t.Fatalf("got %d; read %d bytes", got, reader.bytesRead)
			}
		})
	}
}
func TestSummarizeParquetFooters(t *testing.T) {
	files := []parquetFooterFile{{FileBytes: 100, MetadataBytes: 2, FooterBytes: 10}, {FileBytes: 300, MetadataBytes: 22, FooterBytes: 30}, {FileBytes: 1000, Error: "failed"}}
	s := summarizeParquetFooters(files)
	if s.Files != 2 || s.FooterBytes != 40 || s.MetadataBytes != 24 || s.FileBytes != 400 || s.FooterPercent != 10 || s.Mean != 20 || s.Min != 10 || s.Max != 30 || s.P50 != 10 || s.P95 != 30 {
		t.Fatalf("unexpected summary: %+v", s)
	}
	if empty := summarizeParquetFooters(nil); empty.Files != 0 || empty.FooterPercent != 0 {
		t.Fatalf("unexpected empty summary: %+v", empty)
	}
}
