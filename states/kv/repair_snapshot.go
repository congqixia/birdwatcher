package kv

import (
	"context"
	"fmt"
	"sort"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/models"
)

// EtcdRepairSnapshot pins the inputs of a repair to one etcd revision.
type EtcdRepairSnapshot struct {
	Entries map[string]*mvccpb.KeyValue
}

func repairEtcd(cli MetaKV) (*etcdKV, error) {
	if audit, ok := cli.(*FileAuditKV); ok {
		return repairEtcd(audit.cli)
	}
	if etcd, ok := cli.(*etcdKV); ok {
		return etcd, nil
	}
	return nil, fmt.Errorf("repair requires a live etcd connection (audit wrappers supported)")
}

// ReadEtcdRepairSnapshot reads all prefixes in one read-only transaction.
func ReadEtcdRepairSnapshot(ctx context.Context, cli MetaKV, prefixes []string) (*EtcdRepairSnapshot, error) {
	etcd, err := repairEtcd(cli)
	if err != nil {
		return nil, err
	}
	ops := make([]clientv3.Op, 0, len(prefixes))
	for _, prefix := range prefixes {
		prefix = joinPath(etcd.rootPath, prefix)
		ops = append(ops, clientv3.OpGet(prefix, clientv3.WithPrefix()))
	}
	resp, err := etcd.client.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		return nil, fmt.Errorf("read repair snapshot: %w", err)
	}
	snapshot := &EtcdRepairSnapshot{Entries: make(map[string]*mvccpb.KeyValue)}
	for _, response := range resp.Responses {
		for _, entry := range response.GetResponseRange().Kvs {
			snapshot.Entries[string(entry.Key)] = entry
		}
	}
	return snapshot, nil
}

// ApplyEtcdRepair validates the whole plan, then updates each record with its
// own CAS. A failure leaves confirmed commits intact so a fresh plan can resume.
func ApplyEtcdRepair(ctx context.Context, cli MetaKV, snapshot *EtcdRepairSnapshot, dependencies map[string][]string, updates map[string]string) (int, error) {
	if len(updates) == 0 {
		return 0, nil
	}
	etcd, err := repairEtcd(cli)
	if err != nil {
		return 0, err
	}
	keys := make([]string, 0, len(updates))
	comparisons := make(map[string][]clientv3.Cmp, len(updates))
	for key := range updates {
		keys = append(keys, key)
		expected := append([]string{key}, dependencies[key]...)
		for _, dependency := range expected {
			entry, ok := snapshot.Entries[dependency]
			if !ok {
				return 0, fmt.Errorf("repair dependency %s is absent from the snapshot", dependency)
			}
			comparisons[key] = append(comparisons[key], clientv3.Compare(clientv3.ModRevision(dependency), "=", entry.ModRevision))
		}
	}
	sort.Strings(keys)
	committed := 0
	for _, key := range keys {
		err := auditEtcdRepair(cli, key, updates[key], func() error {
			resp, err := etcd.client.Txn(ctx).If(comparisons[key]...).Then(clientv3.OpPut(key, updates[key])).Commit()
			if err != nil {
				return fmt.Errorf("write %s failed; its outcome may be unknown, inspect this key before retrying: %w", key, err)
			}
			if !resp.Succeeded {
				return fmt.Errorf("metadata changed for %s or its dependencies; rerun the command", key)
			}
			return nil
		})
		if err != nil {
			return committed, err
		}
		committed++
		resp, err := etcd.client.Get(ctx, key)
		if err != nil {
			return committed, fmt.Errorf("repair committed but readback failed for %s; inspect before retrying: %w", key, err)
		}
		if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != updates[key] {
			return committed, fmt.Errorf("repair committed but readback differs for %s; inspect before retrying", key)
		}
	}
	return committed, nil
}

func auditEtcdRepair(cli MetaKV, key, value string, commit func() error) error {
	audit, ok := cli.(*FileAuditKV)
	if !ok {
		return commit()
	}
	audit.writeHeader(models.AuditOpType_OpPut, 2)
	err := auditEtcdRepair(audit.cli, key, value, commit)
	if err == nil {
		audit.writeHeader(models.AuditOpType_OpPutBefore, 1)
		audit.writeKeyValue(key, value)
	}
	audit.writeHeader(models.AuditOpType_OpPutAfter, 1)
	return err
}
