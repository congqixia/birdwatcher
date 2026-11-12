package kv

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gosuri/uilive"
	tikv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/txnkv"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

const (
	// We are using a Snapshot instead of transaction when doing read only operations due to the
	// lower overhead (50% less overhead in small tests). In order to guarantee the latest values are
	// grabbed at call, we can set the TS to be the max uint64.
	MaxSnapshotTS = uint64(math.MaxUint64)
	// For reads by prefix we can customize the scan size to increase/decrease rpc calls.
	SnapshotScanSize = 100
	// etcdLoadWithPrefixPageSize bounds each LoadWithPrefix Range page. Pages
	// must stay small enough to never trip the gRPC transport message size
	// limits.
	etcdLoadWithPrefixPageSize = 100
	// This empty value is what we are reserving within TiKv to represent an empty string value.
	// TiKV does not allow storing empty values for keys which is something we do in Milvus, so
	// to get over this we are using the reserved keyword as placeholder.
	EmptyValueString = "__milvus_reserved_empty_tikv_value_DO_NOT_USE"
	// RequestTimeout is the default timeout for tikv request.
	RequestTimeout = time.Second * 3
)

var (
	EmptyValueByte = []byte(EmptyValueString)
	ErrKeyNotFound = errors.New("key not found")

	// ErrGetStreamUnsupported is returned by MetaKV.GetStream for backends that
	// do not support the streaming range API (e.g. TiKV).
	ErrGetStreamUnsupported = errors.New("GetStream is not supported by this backend")
)

// RangeStreamChunk is a chunk of keys returned by MetaKV.GetStream. Err carries
// a terminal stream error when non-nil.
type RangeStreamChunk struct {
	Keys [][]byte
	More bool
	Err  error
}

// MetaKV contains base operations of kv. Include save, load and remove etc.
type MetaKV interface {
	Load(ctx context.Context, key string, opts ...LoadOption) (string, error)
	LoadWithPrefix(ctx context.Context, key string, opts ...LoadOption) ([]string, []string, error)
	Save(ctx context.Context, key, value string) error
	MultiSave(ctx context.Context, keys, values []string) error
	Remove(ctx context.Context, key string) error
	RemoveWithPrefix(ctx context.Context, key string) error
	removeWithPrevKV(ctx context.Context, key string) (*mvccpb.KeyValue, error)
	removeWithPrefixAndPrevKV(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error)
	GetAllRootPath(ctx context.Context) ([]string, error)
	GetStream(ctx context.Context, key string, opts ...clientv3.OpOption) (<-chan RangeStreamChunk, error)
	BackupKV(base, prefix string, w *bufio.Writer, ignoreRevision bool, batchSize int64) error
	WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error
	Close()
}

func MustGetETCDClient(kv MetaKV) *clientv3.Client {
	etcd := kv.(*etcdKV)
	return etcd.client
}

// implementation assertion
var _ MetaKV = (*etcdKV)(nil)

// etcdKV implements TxnKV interface, it supports to process multiple kvs in a transaction.
type etcdKV struct {
	client   *clientv3.Client
	rootPath string
}

// NewEtcdKV creates a new etcd kv.
func NewEtcdKV(client *clientv3.Client) *etcdKV {
	kv := &etcdKV{
		client:   client,
		rootPath: "",
	}
	return kv
}

// Load returns value of the key.
func (kv *etcdKV) Load(ctx context.Context, key string, opts ...LoadOption) (string, error) {
	opt := defaultLoadOption()
	for _, f := range opts {
		f(opt)
	}
	key = joinPath(kv.rootPath, key)
	resp, err := kv.client.Get(ctx, key, opt.EtcdOptions()...)
	if err != nil {
		return "", err
	}
	if resp.Count <= 0 {
		return "", ErrKeyNotFound
	}
	return string(resp.Kvs[0].Value), nil
}

// LoadWithPrefix returns all the keys and values with the given key prefix.
//
// The prefix can span multiple etcd Range pages. A single unbounded Get either
// returns every matching key at once or fails with ResourceExhausted when the
// response exceeds the transport message size budget (etcd server send cap /
// gRPC recv limit) — it never silently truncates. Loop over Limit-bounded
// pages so callers always receive the complete, sorted listing.
//
// An empty prefix scans the whole keyspace, matching the old WithPrefix()
// translation: etcd treats "\x00" as the keyspace-low start key and a "\x00"
// range end as "to the end of the keyspace".
func (kv *etcdKV) LoadWithPrefix(ctx context.Context, key string, opts ...LoadOption) ([]string, []string, error) {
	opt := defaultLoadOption()
	for _, f := range opts {
		f(opt)
	}
	key = joinPath(kv.rootPath, key)
	// Keep the original range end; pagination only advances the start key.
	// WithSort + WithLimit is honored by etcd (fetch all, sort, then truncate
	// to Limit and set More), so each page is bounded in size.
	options := []clientv3.OpOption{
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(key)),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(etcdLoadWithPrefixPageSize),
	}
	options = append(options, opt.EtcdOptions()...)

	var keys, values []string
	// etcd rejects a range with an empty start key. For an empty prefix, start
	// at "\x00" (the keyspace low) so the range covers every key.
	next := key
	if next == "" {
		next = "\x00"
	}
	// rev pins every page to the first page's revision so the combined listing
	// reflects a single snapshot, like the original single unbounded Range did.
	var rev int64
	for {
		opts := options
		if rev != 0 {
			opts = append(opts, clientv3.WithRev(rev))
		}
		resp, err := kv.client.Get(ctx, next, opts...)
		if err != nil {
			return nil, nil, err
		}
		if rev == 0 {
			rev = resp.Header.Revision
		}
		if keys == nil {
			// Count is the total number of matching keys in the range even
			// when the first page is bounded, so it is a safe preallocation
			// hint across all pages.
			keys = make([]string, 0, resp.Count)
			values = make([]string, 0, resp.Count)
		}
		for _, item := range resp.Kvs {
			keys = append(keys, string(item.Key))
			values = append(values, string(item.Value))
		}
		if !resp.More {
			break
		}
		if len(resp.Kvs) == 0 {
			return nil, nil, fmt.Errorf("etcd LoadWithPrefix: More set but empty page at %q", next)
		}
		// '\x00' is the immediate lexicographic successor of the last key; the
		// next page starts right after it and stays inside the range.
		next = string(append(resp.Kvs[len(resp.Kvs)-1].Key, 0))
	}
	return keys, values, nil
}

// Save saves the key-value pair.
func (kv *etcdKV) Save(ctx context.Context, key, value string) error {
	key = joinPath(kv.rootPath, key)
	_, err := kv.client.Put(ctx, key, value)
	return err
}

func (kv *etcdKV) MultiSave(ctx context.Context, keys, values []string) error {
	var ops []clientv3.Op
	for i, key := range keys {
		ops = append(ops, clientv3.OpPut(key, values[i]))
	}

	_, err := kv.client.Txn(ctx).If().Then(ops...).Commit()
	return err
}

// Remove removes the key.
func (kv *etcdKV) Remove(ctx context.Context, key string) error {
	key = joinPath(kv.rootPath, key)
	_, err := kv.client.Delete(ctx, key)
	return err
}

// RemoveWithPrefix removes the keys with given prefix.
func (kv *etcdKV) RemoveWithPrefix(ctx context.Context, prefix string) error {
	key := joinPath(kv.rootPath, prefix)
	// preix must end with "/" in case of remove unwanted keys
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	_, err := kv.client.Delete(ctx, key, clientv3.WithPrefix())
	return err
}

func (kv *etcdKV) removeWithPrevKV(ctx context.Context, key string) (*mvccpb.KeyValue, error) {
	key = joinPath(kv.rootPath, key)
	resp, err := kv.client.Delete(ctx, key, clientv3.WithPrevKV())
	if err != nil {
		return nil, err
	}
	if len(resp.PrevKvs) > 0 {
		return resp.PrevKvs[0], nil
	}
	// Note: we allow response to be empty, which suggests that the key doesn't exist e.g. it's already deleted.
	return nil, nil
}

func (kv *etcdKV) removeWithPrefixAndPrevKV(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error) {
	key := joinPath(kv.rootPath, prefix)
	resp, err := kv.client.Delete(ctx, key, clientv3.WithPrefix(), clientv3.WithPrevKV())
	return resp.PrevKvs, err
}

// GetStream streams keys in ascending order starting at key via the etcd
// RangeStream RPC (etcd >= 3.7). The stream is delivered in chunks; the caller
// must drain the returned channel and check each chunk's Err field for a
// terminal error. Returns ErrGetStreamUnsupported on backends without support.
func (kv *etcdKV) GetStream(ctx context.Context, key string, opts ...clientv3.OpOption) (<-chan RangeStreamChunk, error) {
	stream, err := kv.client.GetStream(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	ch := make(chan RangeStreamChunk, 1)
	go func() {
		defer close(ch)
		for resp := range stream {
			chunk := RangeStreamChunk{}
			if resp.Err() != nil {
				chunk.Err = resp.Err()
			} else {
				for _, kv := range resp.Kvs {
					chunk.Keys = append(chunk.Keys, kv.Key)
				}
				chunk.More = resp.More
			}
			ch <- chunk
		}
	}()
	return ch, nil
}

// GetAllRootPath enumerates all possible root paths. It prefers the streaming
// Range API and falls back to the legacy unary pagination when the connected
// etcd server does not support the RangeStream RPC.
func (kv *etcdKV) GetAllRootPath(ctx context.Context) ([]string, error) {
	apps, err := kv.getRootPathsStream(ctx)
	if err == nil {
		return apps, nil
	}
	if !isRangeStreamUnsupported(err) {
		return nil, err
	}
	return kv.getRootPathsUnary(ctx)
}

func isRangeStreamUnsupported(err error) bool {
	return errors.Is(err, ErrGetStreamUnsupported) || status.Code(err) == codes.Unimplemented
}

// getRootPathsStream enumerates root paths with one RangeStream request per
// first-level prefix. On etcd >= 3.7 each request stops the in-memory index
// traversal right after the requested limit, so the cost is proportional to
// the number of prefixes instead of the whole keyspace.
func (kv *etcdKV) getRootPathsStream(ctx context.Context) ([]string, error) {
	var apps []string
	current := ""
	for {
		stream, err := kv.GetStream(ctx, current, clientv3.WithKeysOnly(), clientv3.WithLimit(1), clientv3.WithFromKey())
		if err != nil {
			return nil, err
		}
		found := false
		for chunk := range stream {
			if chunk.Err != nil {
				return nil, chunk.Err
			}
			if len(chunk.Keys) == 0 {
				continue
			}
			key := string(chunk.Keys[0])
			parts := strings.Split(key, "/")
			if parts[0] != "" {
				apps = append(apps, parts[0])
			}
			// next key, since '0' is the next ascii char of '/'
			current = parts[0] + "0"
			found = true
		}
		if !found {
			break
		}
	}
	return apps, nil
}

// getRootPathsUnary is the legacy root path enumeration using unary Range
// requests. Each request walks the remaining in-memory index on the server,
// which is expensive on large shared etcd clusters; kept for etcd < 3.7.
func (kv *etcdKV) getRootPathsUnary(ctx context.Context) ([]string, error) {
	var apps []string
	current := ""
	for {
		resp, err := kv.client.Get(ctx, current, clientv3.WithKeysOnly(), clientv3.WithLimit(1), clientv3.WithFromKey())
		if err != nil {
			return nil, err
		}
		for _, kvs := range resp.Kvs {
			key := string(kvs.Key)
			parts := strings.Split(key, "/")
			if parts[0] != "" {
				apps = append(apps, parts[0])
			}
			// next key, since '0' is the next ascii char of '/'
			current = parts[0] + "0"
		}

		if !resp.More {
			break
		}
	}

	return apps, nil
}

func writeBackupBytes(w *bufio.Writer, data []byte) {
	lb := make([]byte, 8)
	binary.LittleEndian.PutUint64(lb, uint64(len(data)))
	w.Write(lb)
	if len(data) > 0 {
		w.Write(data)
	}
}

func (kv *etcdKV) BackupKV(base, prefix string, w *bufio.Writer, ignoreRevision bool, batchSize int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := kv.client.Get(ctx, joinPath(base, prefix), clientv3.WithCountOnly(), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	if ignoreRevision {
		fmt.Println("WARNING!!! doing backup ignore revision! please make sure no instance of milvus is online!")
	}

	// meta stored in extra
	meta := make(map[string]string)

	cnt := resp.Count
	rev := resp.Header.Revision
	meta["cnt"] = fmt.Sprintf("%d", cnt)
	meta["rev"] = fmt.Sprintf("%d", rev)
	var instance, metaPath string
	parts := strings.Split(base, "/")
	if len(parts) > 1 {
		metaPath = parts[len(parts)-1]
		instance = joinPath(parts[:len(parts)-1]...)
	} else {
		instance = base
	}
	meta["instance"] = instance
	meta["metaPath"] = metaPath

	fmt.Println("meta path ", metaPath)

	bs, _ := json.Marshal(meta)
	ph := models.PartHeader{
		PartType: models.PartType_EtcdBackup,
		PartLen:  -1, // not sure for length
		Extra:    bs,
	}
	bs, err = proto.Marshal(&ph)
	if err != nil {
		fmt.Println("failed to marshal part header for etcd backup", err.Error())
		return err
	}
	writeBackupBytes(w, bs)

	progressDisplay := uilive.New()
	progressFmt := "Backing up etcd ... %d%%(%d/%d)\n"
	progressDisplay.Start()
	fmt.Fprintf(progressDisplay, progressFmt, 0, 0, cnt)

	options := []clientv3.OpOption{clientv3.WithFromKey(), clientv3.WithLimit(batchSize)}
	if !ignoreRevision {
		options = append(options, clientv3.WithRev(rev))
	}

	currentKey := joinPath(base, prefix)
	var i int
	prefixBS := []byte(currentKey)
	for {
		resp, err = kv.client.Get(context.Background(), currentKey, options...)
		if err != nil {
			return err
		}

		if resp.Count == 0 {
			break
		}

		valid := 0
		for _, kvs := range resp.Kvs {
			if !bytes.HasPrefix(kvs.Key, prefixBS) {
				continue
			}
			valid++
			entry := &commonpb.KeyDataPair{Key: string(kvs.Key), Data: kvs.Value}
			bs, err = proto.Marshal(entry)
			if err != nil {
				fmt.Println("failed to marshal kv pair", err.Error())
				return err
			}
			writeBackupBytes(w, bs)

			currentKey = string(append(kvs.Key, 0))
		}
		i += valid

		progress := i * 100 / int(cnt)
		fmt.Fprintf(progressDisplay, progressFmt, progress, i, cnt)
	}
	w.Flush()
	progressDisplay.Stop()

	// write stopper
	writeBackupBytes(w, nil)

	w.Flush()

	fmt.Printf("backup etcd for prefix %s done\n", prefix)
	return nil
}

func (kv *etcdKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	prefix = path.Join(kv.rootPath, prefix)

	batch := int64(paginationSize)
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(batch),
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)),
	}

	key := prefix
	for {
		resp, err := kv.client.Get(ctx, key, opts...)
		if err != nil {
			return err
		}

		for _, kv := range resp.Kvs {
			if err = fn(kv.Key, kv.Value); err != nil {
				return err
			}
		}

		if !resp.More {
			break
		}
		// move to next key
		key = string(append(resp.Kvs[len(resp.Kvs)-1].Key, 0))
	}

	return nil
}

// Close closes the connection to etcd.
func (kv *etcdKV) Close() {
	kv.client.Close()
}

// implementation assertion
var _ MetaKV = (*txnTiKV)(nil)

// txnTiKV implements MetaKV and TxnKV interface. It supports processing multiple kvs within one transaction.
type txnTiKV struct {
	client   *txnkv.Client
	rootPath string
}

// Since TiKV cannot store empty key values, we assign them a placeholder held by EmptyValue.
// Upon loading, we need to check if the returned value is the placeholder.
func isEmptyByte(value []byte) bool {
	return bytes.Equal(value, EmptyValueByte) || len(value) == 0
}

// Return an empty string if the value is the Empty placeholder, else return actual string value.
func convertEmptyByteToString(value []byte) string {
	if isEmptyByte(value) {
		return ""
	}
	return string(value)
}

// Convert string into EmptyValue if empty else cast to []byte. Will throw error if value is equal
// to the EmptyValueString.
func convertEmptyStringToByte(value string) ([]byte, error) {
	if len(value) == 0 {
		return EmptyValueByte, nil
	} else if value == EmptyValueString {
		return nil, fmt.Errorf("value for key is reserved by EmptyValue: %s", EmptyValueString)
	}
	return []byte(value), nil
}

// NewTiKV creates a new txnTiKV client.
func NewTiKV(txn *txnkv.Client) *txnTiKV {
	kv := &txnTiKV{
		client:   txn,
		rootPath: "",
	}
	return kv
}

// Load returns value of the key.
func (kv *txnTiKV) Load(ctx context.Context, key string, opts ...LoadOption) (string, error) {
	key = joinPath(kv.rootPath, key)

	// TODO handle load option
	ss := kv.client.GetSnapshot(MaxSnapshotTS)
	ss.SetScanBatchSize(SnapshotScanSize)

	val, err := ss.Get(ctx, []byte(key))
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("Failed to load value for key %s", key))
	}

	// Check if value is the empty placeholder
	value := convertEmptyByteToString(val.Value)

	return value, err
}

// LoadWithPrefix returns all the keys and values for the given key prefix.
func (kv *txnTiKV) LoadWithPrefix(ctx context.Context, prefix string, opts ...LoadOption) ([]string, []string, error) {
	prefix = joinPath(kv.rootPath, prefix)

	// TODO handle load option
	ss := kv.client.GetSnapshot(MaxSnapshotTS)
	ss.SetScanBatchSize(SnapshotScanSize)

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Failed to create iterater for LoadWithPrefix() for prefix: %s", prefix))
		return nil, nil, err
	}
	defer iter.Close()

	var keys []string
	var values []string

	// Iterate over the key-value pairs
	for iter.Valid() {
		val := iter.Value()
		// Check if empty value placeholder
		strVal := convertEmptyByteToString(val)
		keys = append(keys, string(iter.Key()))
		values = append(values, strVal)
		err = iter.Next()
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Failed to iterate for LoadWithPrefix() for prefix: %s", prefix))
			return nil, nil, err
		}
	}
	return keys, values, nil
}

// Save saves the input key-value pair.
func (kv *txnTiKV) Save(ctx context.Context, key, value string) error {
	key = joinPath(kv.rootPath, key)

	txn, err := kv.client.Begin()
	if err != nil {
		return errors.Wrap(err, "Failed to build transaction for putTiKVMeta")
	}

	// Check if the value being written needs to be empty plaeholder
	byteValue, err := convertEmptyStringToByte(value)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to cast to byte (%s:%s) for putTiKVMeta", key, value))
	}
	err = txn.Set([]byte(key), byteValue)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to set value for key %s in putTiKVMeta", key))
	}
	return txn.Commit(ctx)
}

func (kv *txnTiKV) MultiSave(ctx context.Context, keys, values []string) error {
	return errors.New("not implemented")
}

// Remove removes the input key.
func (kv *txnTiKV) Remove(ctx context.Context, key string) error {
	key = joinPath(kv.rootPath, key)

	txn, err := kv.client.Begin()
	if err != nil {
		return errors.Wrap(err, "Failed to build transaction for removeTiKVMeta")
	}

	err = txn.Delete([]byte(key))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("Failed to remove key %s in removeTiKVMeta", key))
	}
	return txn.Commit(ctx)
}

// RemoveWithPrefix removes the keys for the given prefix.
func (kv *txnTiKV) RemoveWithPrefix(ctx context.Context, prefix string) error {
	prefix = joinPath(kv.rootPath, prefix)
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey(startKey)
	_, err := kv.client.DeleteRange(ctx, startKey, endKey, 1)
	return err
}

func (kv *txnTiKV) removeWithPrevKV(ctx context.Context, key string) (*mvccpb.KeyValue, error) {
	preV, err := kv.Load(ctx, key)
	if err != nil {
		// Note: we allow response to be empty, which suggests that the key doesn't exist e.g. it's already deleted.
		return nil, nil
	}
	err = kv.Remove(ctx, key)
	if err != nil {
		return nil, err
	}
	pkv := &mvccpb.KeyValue{
		Key:   []byte(key),
		Value: []byte(preV),
	}
	return pkv, nil
}

func (kv *txnTiKV) removeWithPrefixAndPrevKV(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error) {
	keys, vals, err := kv.LoadWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	err = kv.RemoveWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	var kvs []*mvccpb.KeyValue
	for i, key := range keys {
		pkv := &mvccpb.KeyValue{
			Key:   []byte(key),
			Value: []byte(vals[i]),
		}
		kvs = append(kvs, pkv)
	}
	return kvs, nil
}

func (kv *txnTiKV) GetAllRootPath(ctx context.Context) ([]string, error) {
	var apps []string
	ss := kv.client.GetSnapshot(MaxSnapshotTS)
	ss.SetScanBatchSize(SnapshotScanSize)

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(" ")
	endKey := []byte("~")
	for {
		iter, err := ss.Iter(startKey, endKey)
		if err != nil {
			return nil, err
		}
		defer iter.Close()

		if iter.Valid() {
			key := string(iter.Key())
			parts := strings.Split(key, "/")
			if parts[0] != "" {
				apps = append(apps, parts[0])
			} else {
				break
			}
			// next key, since '0' is the next ascii char of '/'
			startKey = []byte(parts[0] + "0")
		} else {
			break
		}
	}
	return apps, nil
}

// GetStream is not supported by the TiKV backend.
func (kv *txnTiKV) GetStream(ctx context.Context, key string, opts ...clientv3.OpOption) (<-chan RangeStreamChunk, error) {
	return nil, ErrGetStreamUnsupported
}

func (kv *txnTiKV) BackupKV(base, prefix string, w *bufio.Writer, ignoreRevision bool, batchSize int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	txn, err := kv.client.Begin()
	if err != nil {
		return errors.Wrap(err, "Failed to build transaction for removeTiKVMeta")
	}
	ss := txn.GetSnapshot()
	ss.SetScanBatchSize(SnapshotScanSize)

	keyprefix := joinPath(base, prefix)
	startKey := []byte(keyprefix)
	endKey := tikv.PrefixNextKey([]byte(keyprefix))
	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		return err
	}

	cnt := 0
	for iter.Valid() {
		cnt++
		err = iter.Next()
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Failed to iterate for BackupKV() count for prefix: %s, base: %s", prefix, base))
		}
	}

	// meta stored in extra
	meta := make(map[string]string)

	meta["cnt"] = fmt.Sprintf("%d", cnt)
	meta["rev"] = "0"
	var instance, metaPath string
	parts := strings.Split(base, "/")
	if len(parts) > 1 {
		metaPath = parts[len(parts)-1]
		instance = joinPath(parts[:len(parts)-1]...)
	} else {
		instance = base
	}
	meta["instance"] = instance
	meta["metaPath"] = metaPath

	bs, _ := json.Marshal(meta)
	ph := models.PartHeader{
		PartType: models.PartType_EtcdBackup,
		PartLen:  -1, // not sure for length
		Extra:    bs,
	}
	bs, err = proto.Marshal(&ph)
	if err != nil {
		fmt.Println("failed to marshal part header for etcd backup", err.Error())
		return err
	}
	writeBackupBytes(w, bs)

	progressDisplay := uilive.New()
	progressFmt := "Backing up tikv ... %d%%(%d/%d)\n"
	progressDisplay.Start()
	fmt.Fprintf(progressDisplay, progressFmt, 0, 0, cnt)

	iter, err = ss.Iter(startKey, endKey)
	if err != nil {
		return err
	}
	i := 0
	prefixBS := []byte(base)
	for iter.Valid() {
		if !bytes.HasPrefix(iter.Key(), prefixBS) {
			continue
		}
		entry := &commonpb.KeyDataPair{Key: string(iter.Key()), Data: iter.Value()}
		bs, err = proto.Marshal(entry)
		if err != nil {
			fmt.Println("failed to marshal kv pair", err.Error())
			return err
		}
		writeBackupBytes(w, bs)
		err = iter.Next()
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("Failed to iterate for BackupKV() for prefix: %s, base: %s", prefix, base))
		}

		i++
		progress := i * 100 / cnt
		if i%int(batchSize) == 0 {
			fmt.Fprintf(progressDisplay, progressFmt, progress, i, cnt)
		}
	}
	fmt.Fprintf(progressDisplay, progressFmt, (i * 100 / cnt), i, cnt)

	w.Flush()
	progressDisplay.Stop()

	// write stopper
	writeBackupBytes(w, nil)

	w.Flush()

	fmt.Printf("backup tikv for prefix %s done\n", prefix)
	return txn.Commit(ctx)
}

func (kv *txnTiKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	prefix = path.Join(kv.rootPath, prefix)

	// Since only reading, use Snapshot for less overhead
	ss := kv.client.GetSnapshot(MaxSnapshotTS)
	ss.SetScanBatchSize(paginationSize)

	// Retrieve key-value pairs with the specified prefix
	startKey := []byte(prefix)
	endKey := tikv.PrefixNextKey([]byte(prefix))
	iter, err := ss.Iter(startKey, endKey)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Failed to create iterater for %s during WalkWithPrefix", prefix))
		return err
	}
	defer iter.Close()

	// Iterate over the key-value pairs
	for iter.Valid() {
		// Grab value for empty check
		byteVal := iter.Value()
		// Check if empty val and replace with placeholder
		if isEmptyByte(byteVal) {
			byteVal = []byte{}
		}
		err = fn(iter.Key(), byteVal)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Failed to apply fn to (%s;%s)", string(iter.Key()), string(byteVal)))
			return err
		}
		err = iter.Next()
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Failed to move Iterator after key %s for WalkWithPrefix", string(iter.Key())))
			return err
		}
	}
	return nil
}

// Close closes the connection to TiKV.
func (kv *txnTiKV) Close() {
	kv.client.Close()
}
