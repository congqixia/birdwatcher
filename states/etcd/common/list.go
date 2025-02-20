package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/proto"
	protov2 "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/runtime/protoiface"

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
	protoiface.MessageV1
}](ctx context.Context, kv kv.MetaKV, prefix string, filters ...func(t *T) bool) ([]T, []string, error) {
	keys, vals, err := kv.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, nil, err
	}
	if len(keys) != len(vals) {
		return nil, nil, fmt.Errorf("Error: keys and vals of different size in ListProtoObjects:%d vs %d", len(keys), len(vals))
	}
	result := make([]T, 0, len(keys))
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
		result = append(result, elem)
	}
	return result, keys, nil
}

// ListProtoObjects returns proto objects with specified prefix.
func ListProtoObjectsV2[T any, P interface {
	*T
	protov2.Message
}](ctx context.Context, kv kv.MetaKV, prefix string, filters ...func(t *T) bool) ([]T, []string, error) {
	keys, vals, err := kv.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, nil, err
	}
	if len(keys) != len(vals) {
		return nil, nil, fmt.Errorf("error: keys and vals of different size in ListProtoObjects:%d vs %d", len(keys), len(vals))
	}
	result := make([]T, 0, len(keys))
LOOP:
	for idx, val := range vals {
		var elem T
		info := P(&elem)
		err = protov2.Unmarshal([]byte(val), info)
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
		result = append(result, elem)
	}
	return result, keys, nil
}

// ListProtoObjectsAdv returns proto objects with specified prefix.
// add preFilter to handle tombstone cases.
func ListProtoObjectsAdv[T any, P interface {
	*T
	protoiface.MessageV1
}](ctx context.Context, kv kv.MetaKV, prefix string, preFilter func(string, []byte) bool, filters ...func(t *T) bool) ([]T, []string, error) {
	keys, vals, err := kv.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, nil, err
	}
	if len(keys) != len(vals) {
		return nil, nil, fmt.Errorf("Error: keys and vals of different size in ListProtoObjectsAdv:%d vs %d", len(keys), len(vals))
	}
	result := make([]T, 0, len(vals))
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
		result = append(result, elem)
	}
	return result, keys, nil
}
