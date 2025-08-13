package kv

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"

	"github.com/milvus-io/birdwatcher/models"
)

// implementation assertion
var _ MetaKV = (*FileAuditKV)(nil)

type FileAuditKV struct {
	cli  MetaKV
	file *os.File
}

// NewFileAuditKV creates a file auditing log kv.
func NewFileAuditKV(kv MetaKV, file *os.File) *FileAuditKV {
	return &FileAuditKV{
		cli:  kv,
		file: file,
	}
}

func (c *FileAuditKV) Load(ctx context.Context, key string, opts ...LoadOption) (string, error) {
	return c.cli.Load(ctx, key, opts...)
}

func (c *FileAuditKV) LoadWithPrefix(ctx context.Context, key string, opts ...LoadOption) ([]string, []string, error) {
	return c.cli.LoadWithPrefix(ctx, key, opts...)
}

func (c *FileAuditKV) CountWithPrefix(ctx context.Context, key string) (int64, error) {
	return c.cli.CountWithPrefix(ctx, key)
}

func (c *FileAuditKV) Save(ctx context.Context, key, value string) error {
	c.writeHeader(models.AuditOpType_OpPut, 2)
	err := c.cli.Save(ctx, key, value)
	if err == nil {
		c.writeHeader(models.AuditOpType_OpPutBefore, 1)
		c.writeKeyValue(key, value)
	}
	c.writeHeader(models.AuditOpType_OpPutAfter, 1)
	return err
}

func (c *FileAuditKV) MultiSave(ctx context.Context, keys, values []string) error {
	return errors.New("not implemented")
}

func (c *FileAuditKV) Remove(ctx context.Context, key string) error {
	fmt.Println("audit delete", key)
	val, err := c.cli.Load(ctx, key)
	if err != nil {
		return err
	}

	err = c.cli.Remove(ctx, key)
	if err != nil {
		return err
	}
	c.writeHeader(models.AuditOpType_OpDel, 1)
	c.writeKeyValue(key, val)
	return nil
}

func (c *FileAuditKV) RemoveWithPrefix(ctx context.Context, key string) error {
	fmt.Println("audit delete with prefix", key)
	keys, values, err := c.cli.LoadWithPrefix(ctx, key)
	if err != nil {
		return err
	}

	err = c.cli.RemoveWithPrefix(ctx, key)
	if err != nil {
		return err
	}
	c.writeHeader(models.AuditOpType_OpDel, 1)
	for i, key := range keys {
		val := values[i]
		c.writeKeyValue(key, val)
	}
	return nil
}

func (c *FileAuditKV) removeWithPrevKV(ctx context.Context, key string) (*mvccpb.KeyValue, error) {
	return c.cli.removeWithPrevKV(ctx, key)
}

func (c *FileAuditKV) removeWithPrefixAndPrevKV(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error) {
	return c.cli.removeWithPrefixAndPrevKV(ctx, prefix)
}

func (c *FileAuditKV) GetAllRootPath(ctx context.Context) ([]string, error) {
	return c.cli.GetAllRootPath(ctx)
}

func (c *FileAuditKV) Close() {
	c.cli.Close()
}

func (c *FileAuditKV) BackupKV(base, prefix string, w *bufio.Writer, ignoreRevision bool, batchSize int64) error {
	return c.cli.BackupKV(base, prefix, w, ignoreRevision, batchSize)
}

func (c *FileAuditKV) writeHeader(op models.AuditOpType, entriesNum int32) {
	header := &models.AuditHeader{
		Version:    1,
		OpType:     int32(op),
		EntriesNum: entriesNum,
	}
	bs, _ := proto.Marshal(header)
	c.writeData(bs)
}

func (c *FileAuditKV) writeLogKV(kv *mvccpb.KeyValue) {
	if kv == (*mvccpb.KeyValue)(nil) {
		return
	}
	bs, _ := proto.Marshal(protoadapt.MessageV2Of(kv))
	c.writeData(bs)
}

func (c *FileAuditKV) writeKeyValue(key, value string) {
	kv := &mvccpb.KeyValue{
		Key:   []byte(key),
		Value: []byte(value),
	}
	c.writeLogKV(kv)
}

func (c *FileAuditKV) writeData(data []byte) {
	lb := make([]byte, 8)
	binary.LittleEndian.PutUint64(lb, uint64(len(data)))
	_, err := c.file.Write(lb)
	if err != nil {
		fmt.Println("failed to write audit header", err.Error())
		return
	}
	if len(data) > 0 {
		_, err = c.file.Write(data)
		if err != nil {
			fmt.Println("failed to write audit log", err.Error())
		}
	}
}

func (c *FileAuditKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	return c.cli.WalkWithPrefix(ctx, prefix, paginationSize, fn)
}
