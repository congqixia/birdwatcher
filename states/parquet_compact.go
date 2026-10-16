package states

// A bounded Thrift Compact Protocol scanner. Spans refer to original bytes;
// nothing is reserialized, so unknown fields and container overhead are retained.
import (
	"encoding/binary"
	"fmt"
	"io"
)

type compactField struct {
	id    int16
	start int
	value *compactNode
}
type compactNode struct {
	kind       byte
	start, end int
	integer    int64
	binary     []byte
	fields     []compactField
	items      []*compactNode
}

func (n *compactNode) field(id int16) *compactField {
	if n == nil {
		return nil
	}
	for i := range n.fields {
		if n.fields[i].id == id {
			return &n.fields[i]
		}
	}
	return nil
}
func (n *compactNode) child(id int16) *compactNode {
	f := n.field(id)
	if f == nil {
		return nil
	}
	return f.value
}
func compactInt(n *compactNode, id int16) (int64, bool) {
	f := n.child(id)
	if f == nil || f.kind < 4 || f.kind > 6 {
		return 0, false
	}
	return f.integer, true
}
func compactFieldSize(f *compactField) int64 {
	if f == nil {
		return 0
	}
	return int64(f.value.end - f.start)
}

type compactScanner struct {
	data       []byte
	pos, nodes int
}

func (s *compactScanner) take(n int) ([]byte, error) {
	if n < 0 || n > len(s.data)-s.pos {
		return nil, io.ErrUnexpectedEOF
	}
	v := s.data[s.pos : s.pos+n]
	s.pos += n
	return v, nil
}
func (s *compactScanner) byte() (byte, error) {
	b, e := s.take(1)
	if e != nil {
		return 0, e
	}
	return b[0], nil
}
func (s *compactScanner) uint() (uint64, error) {
	v, n := binary.Uvarint(s.data[s.pos:])
	if n == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if n < 0 {
		return 0, fmt.Errorf("compact integer overflow at %d", s.pos)
	}
	s.pos += n
	return v, nil
}
func (s *compactScanner) value(kind byte, depth int) (*compactNode, error) {
	if depth > 64 || s.nodes >= 2_000_000 {
		return nil, fmt.Errorf("compact structure exceeds depth/node limit")
	}
	s.nodes++
	n := &compactNode{start: s.pos, kind: kind}
	var err error
	switch kind {
	case 1, 2:
		n.integer = int64(kind) // struct booleans are encoded in the field header
	case 3:
		var b byte
		b, err = s.byte()
		n.integer = int64(int8(b))
	case 4, 5, 6:
		var v uint64
		v, err = s.uint()
		n.integer = int64(v>>1) ^ -int64(v&1)
	case 7:
		_, err = s.take(8)
	case 8:
		var size uint64
		size, err = s.uint()
		if err == nil {
			if size > uint64(len(s.data)-s.pos) {
				return nil, io.ErrUnexpectedEOF
			}
			n.binary, err = s.take(int(size))
		}
	case 9, 10:
		var head byte
		head, err = s.byte()
		if err != nil {
			return nil, err
		}
		count := uint64(head >> 4)
		element := head & 15
		if count == 15 {
			count, err = s.uint()
			if err != nil {
				return nil, err
			}
		}
		if count > 2_000_000 {
			return nil, fmt.Errorf("compact collection too large")
		}
		for i := uint64(0); i < count; i++ {
			var item *compactNode
			if element == 1 || element == 2 {
				start := s.pos
				var b byte
				b, err = s.byte()
				if err == nil && b != 1 && b != 2 {
					err = fmt.Errorf("invalid compact boolean")
				}
				item = &compactNode{start: start, end: s.pos, integer: int64(b)}
			} else {
				item, err = s.value(element, depth+1)
			}
			if err != nil {
				return nil, err
			}
			n.items = append(n.items, item)
		}
	case 11:
		var count uint64
		count, err = s.uint()
		if err != nil {
			return nil, err
		}
		if count > 1_000_000 {
			return nil, fmt.Errorf("compact map too large")
		}
		if count > 0 {
			var types byte
			types, err = s.byte()
			if err != nil {
				return nil, err
			}
			for i := uint64(0); i < count; i++ {
				for _, typ := range []byte{types >> 4, types & 15} {
					if typ == 1 || typ == 2 {
						var b byte
						b, err = s.byte()
						if err == nil && b != 1 && b != 2 {
							err = fmt.Errorf("invalid compact map boolean")
						}
					} else {
						_, err = s.value(typ, depth+1)
					}
					if err != nil {
						return nil, err
					}
				}
			}
		}
	case 12:
		var last int16
		for {
			start := s.pos
			var head byte
			head, err = s.byte()
			if err != nil {
				return nil, err
			}
			if head == 0 {
				break
			}
			typ := head & 15
			delta := head >> 4
			var id int16
			if delta == 0 {
				var v uint64
				v, err = s.uint()
				if err != nil {
					return nil, err
				}
				signed := int64(v>>1) ^ -int64(v&1)
				if signed < -32768 || signed > 32767 {
					return nil, fmt.Errorf("invalid compact field ID")
				}
				id = int16(signed)
			} else {
				if int(last)+int(delta) > 32767 {
					return nil, fmt.Errorf("compact field ID overflow")
				}
				id = last + int16(delta)
			}
			var child *compactNode
			for _, existing := range n.fields {
				if existing.id == id {
					return nil, fmt.Errorf("duplicate compact field ID %d", id)
				}
			}
			child, err = s.value(typ, depth+1)
			if err != nil {
				return nil, err
			}
			n.fields = append(n.fields, compactField{id: id, start: start, value: child})
			last = id
		}
	case 13:
		_, err = s.take(16)
	default:
		return nil, fmt.Errorf("unsupported compact type %d at %d", kind, s.pos)
	}
	if err != nil {
		return nil, err
	}
	n.end = s.pos
	return n, nil
}
func scanCompactStruct(data []byte) (*compactNode, error) {
	s := compactScanner{data: data}
	return s.value(12, 0)
}
