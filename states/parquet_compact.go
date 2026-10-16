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

// Projection retains only spans needed for footer attribution. In particular,
// schema nodes, encodings, paths, encoding_stats and unknown subtrees are scanned
// and validated without constructing a second in-memory representation of them.
type compactProjection byte

const (
	compactFull compactProjection = iota
	compactSpan
	compactFooter
	compactRowGroups
	compactRowGroup
	compactColumns
	compactColumn
	compactColumnMeta
	compactStats
	compactKVList
	compactKV
)

func (p compactProjection) field(id int16) compactProjection {
	switch p {
	case compactFull:
		return compactFull
	case compactFooter:
		switch id {
		case 4:
			return compactRowGroups
		case 5:
			return compactKVList
		}
	case compactRowGroup:
		if id == 1 {
			return compactColumns
		}
	case compactColumn:
		if id == 3 {
			return compactColumnMeta
		}
	case compactColumnMeta:
		if id == 12 {
			return compactStats
		}
	}
	return compactSpan
}
func (p compactProjection) element() compactProjection {
	switch p {
	case compactFull:
		return compactFull
	case compactRowGroups:
		return compactRowGroup
	case compactColumns:
		return compactColumn
	case compactKVList:
		return compactKV
	}
	return compactSpan
}
func (p compactProjection) keepChildren() bool { return p != compactSpan }

// Keep the allocation inside these helpers: skipped values return by value and
// do not allocate a compactNode on the heap for every visited scalar/container.
func appendCompactField(n *compactNode, id int16, start int, value compactNode) {
	n.fields = append(n.fields, compactField{id: id, start: start, value: &value})
}
func appendCompactItem(n *compactNode, value compactNode) { n.items = append(n.items, &value) }

type compactScanner struct {
	data            []byte
	pos             int
	nodes, maxNodes int64
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
func (s *compactScanner) count() error {
	if s.nodes >= s.maxNodes {
		return fmt.Errorf("compact node limit exceeded: visited=%d limit=%d at byte %d (adjust --maxScanNodes)", s.nodes, s.maxNodes, s.pos)
	}
	s.nodes++
	return nil
}
func (s *compactScanner) value(kind byte, depth int, projection compactProjection) (n compactNode, err error) {
	if depth > 64 {
		return n, fmt.Errorf("compact depth limit exceeded: depth=%d limit=64 at byte %d", depth, s.pos)
	}
	if err = s.count(); err != nil {
		return n, err
	}
	n = compactNode{start: s.pos, kind: kind}
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
				return n, io.ErrUnexpectedEOF
			}
			n.binary, err = s.take(int(size))
		}
	case 9, 10:
		var head byte
		head, err = s.byte()
		if err != nil {
			return n, err
		}
		count := uint64(head >> 4)
		element := head & 15
		if count == 15 {
			count, err = s.uint()
			if err != nil {
				return n, err
			}
		}
		if count > uint64(s.maxNodes-s.nodes) {
			return n, fmt.Errorf("compact collection count %d exceeds remaining node budget %d at byte %d (adjust --maxScanNodes)", count, s.maxNodes-s.nodes, s.pos)
		}
		for i := uint64(0); i < count; i++ {
			var item compactNode
			if element == 1 || element == 2 {
				if err = s.count(); err != nil {
					return n, err
				}
				start := s.pos
				var b byte
				b, err = s.byte()
				if err == nil && b != 1 && b != 2 {
					err = fmt.Errorf("invalid compact boolean")
				}
				item = compactNode{start: start, end: s.pos, kind: element, integer: int64(b)}
			} else {
				item, err = s.value(element, depth+1, projection.element())
			}
			if err != nil {
				return n, err
			}
			if projection.keepChildren() {
				appendCompactItem(&n, item)
			}
		}
	case 11:
		var count uint64
		count, err = s.uint()
		if err != nil {
			return n, err
		}
		if count > uint64((s.maxNodes-s.nodes)/2) {
			return n, fmt.Errorf("compact map count %d exceeds remaining node budget %d at byte %d (adjust --maxScanNodes)", count, s.maxNodes-s.nodes, s.pos)
		}
		if count > 0 {
			var types byte
			types, err = s.byte()
			if err != nil {
				return n, err
			}
			for i := uint64(0); i < count; i++ {
				for _, typ := range []byte{types >> 4, types & 15} {
					if typ == 1 || typ == 2 {
						if err = s.count(); err != nil {
							return n, err
						}
						var b byte
						b, err = s.byte()
						if err == nil && b != 1 && b != 2 {
							err = fmt.Errorf("invalid compact map boolean")
						}
					} else {
						_, err = s.value(typ, depth+1, compactSpan)
					}
					if err != nil {
						return n, err
					}
				}
			}
		}
	case 12:
		var last int16
		seen := make(map[int16]struct{})
		for {
			start := s.pos
			var head byte
			head, err = s.byte()
			if err != nil {
				return n, err
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
					return n, err
				}
				signed := int64(v>>1) ^ -int64(v&1)
				if signed < -32768 || signed > 32767 {
					return n, fmt.Errorf("invalid compact field ID")
				}
				id = int16(signed)
			} else {
				if int(last)+int(delta) > 32767 {
					return n, fmt.Errorf("compact field ID overflow")
				}
				id = last + int16(delta)
			}
			if _, exists := seen[id]; exists {
				return n, fmt.Errorf("duplicate compact field ID %d", id)
			}
			seen[id] = struct{}{}
			var child compactNode
			child, err = s.value(typ, depth+1, projection.field(id))
			if err != nil {
				return n, err
			}
			if projection.keepChildren() {
				appendCompactField(&n, id, start, child)
			}
			last = id
		}
	case 13:
		_, err = s.take(16)
	default:
		return n, fmt.Errorf("unsupported compact type %d at %d", kind, s.pos)
	}
	if err != nil {
		return n, err
	}
	n.end = s.pos
	return n, nil
}
func scanCompactProjected(data []byte, projection compactProjection, maxNodes int64) (*compactNode, error) {
	// Every node consumes at least one input byte (including an inline boolean's
	// field header). This bound accommodates valid large footers without a fixed
	// node cutoff, while keeping malformed declared collection counts bounded.
	if maxNodes == 0 {
		maxNodes = int64(len(data)) + 1
	}
	if maxNodes < 0 {
		return nil, fmt.Errorf("--maxScanNodes must be non-negative")
	}
	s := compactScanner{data: data, maxNodes: maxNodes}
	n, err := s.value(12, 0, projection)
	if err != nil {
		return nil, err
	}
	return &n, nil
}
func scanCompactStruct(data []byte) (*compactNode, error) {
	return scanCompactProjected(data, compactFull, 0)
}
