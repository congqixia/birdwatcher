package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/birdwatcher/states/kv"
)

func ListJSONObjects[T any, P interface{ *T }](ctx context.Context, kv kv.MetaKV, prefix string, filters ...func(t P) bool) ([]P, []string, error) {
	keys, vals, err := kv.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, nil, err
	}
	if len(keys) != len(vals) {
		return nil, nil, fmt.Errorf("Error: keys and vals of different size in ListJSONObjects:%d vs %d", len(keys), len(vals))
	}
	result := make([]P, 0, len(vals))
LOOP:
	for _, val := range vals {
		var elem T
		err = json.Unmarshal([]byte(val), &elem)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		for _, filter := range filters {
			if !filter(&elem) {
				continue LOOP
			}
		}
		result = append(result, &elem)
	}
	return result, keys, nil
}

// ListProtoObjects returns proto objects with specified prefix.
func ListProtoObjects[T any, P interface {
	*T
	proto.Message
}](ctx context.Context, kv kv.MetaKV, prefix string, filters ...func(t *T) bool) ([]*T, []string, error) {
	keys, vals, err := kv.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, nil, err
	}
	if len(keys) != len(vals) {
		return nil, nil, fmt.Errorf("Error: keys and vals of different size in ListProtoObjects:%d vs %d", len(keys), len(vals))
	}
	result := make([]*T, 0, len(keys))
LOOP:
	for idx, val := range vals {
		var elem T
		info := P(&elem)
		err = proto.Unmarshal([]byte(val), info)
		if err != nil {
			if bytes.Equal([]byte(val), []byte{0xE2, 0x9B, 0xBC}) {
				fmt.Printf("Tombstone found, key: %s\n", keys[idx])
				continue
			}
			fmt.Printf("failed to unmarshal key=%s, err: %s\n", keys[idx], err.Error())
			continue
		}

		for _, filter := range filters {
			if !filter(&elem) {
				continue LOOP
			}
		}
		result = append(result, &elem)
	}
	return result, keys, nil
}

// ListProtoObjectsAdv returns proto objects with specified prefix.
// add preFilter to handle tombstone cases.
func ListProtoObjectsAdv[T any, P interface {
	*T
	proto.Message
}](ctx context.Context, kv kv.MetaKV, prefix string, preFilter func(string, []byte) bool, filters ...func(t *T) bool) ([]*T, []string, error) {
	keys, vals, err := kv.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, nil, err
	}
	if len(keys) != len(vals) {
		return nil, nil, fmt.Errorf("Error: keys and vals of different size in ListProtoObjectsAdv:%d vs %d", len(keys), len(vals))
	}
	result := make([]*T, 0, len(vals))
LOOP:
	for i, val := range vals {
		if !preFilter(keys[i], []byte(val)) {
			continue
		}
		var elem T
		info := P(&elem)
		err = proto.Unmarshal([]byte(val), info)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		for _, filter := range filters {
			if !filter(&elem) {
				continue LOOP
			}
		}
		result = append(result, &elem)
	}
	return result, keys, nil
}

func ListObj2Models[T any, proto interface {
	*T
	protoreflect.ProtoMessage
}, M any](ctx context.Context, cli kv.MetaKV, prefix string, convert func(proto, string) *M, filters ...func(*M) bool) ([]*M, error) {
	infos, keys, err := ListProtoObjects[T, proto](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}
	return lo.FilterMap(infos, func(info *T, idx int) (*M, bool) {
		result := convert(info, keys[idx])
		for _, filter := range filters {
			if !filter(result) {
				return nil, false
			}
		}
		return result, true
	}), nil
}

func WalkObjWithPrefix[T any, P interface {
	*T
	protoreflect.ProtoMessage
}, M any](ctx context.Context, cli kv.MetaKV, prefix string, paginationSize int, limit int, convert func(P, string) *M, filters ...func(*M) bool) ([]*M, error) {

	var result []*M
	var cnt int
	err := cli.WalkWithPrefix(ctx, prefix, paginationSize, func(key, value []byte) error {
		var elem T
		info := P(&elem)
		err := proto.Unmarshal(value, info)
		if err != nil {
			fmt.Println(err.Error())
			return nil
		}

		m := convert(info, string(key))
		for _, filter := range filters {
			if !filter(m) {
				return nil
			}
		}
		result = append(result, convert(info, string(key)))
		cnt++
		if limit > 0 && cnt >= limit {
			return ErrReachMaxNumOfWalkSegment
		}
		return nil
	})

	if err != nil && err != ErrReachMaxNumOfWalkSegment {
		return nil, err
	}

	return result, nil
}
