package repair

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func TestWriteRepairedSegment(t *testing.T) {
	t.Run("marshal failure does not write to etcd", func(t *testing.T) {
		cli := &saveRecordingKV{}
		segment := &datapb.SegmentInfo{
			ID:           1,
			CollectionID: 100,
			PartitionID:  10,
			// invalid UTF-8 makes proto.Marshal fail.
			InsertChannel: "\xff\xfe\x00\x80invalid",
		}

		err := writeRepairedSegment(cli, "by-dev/meta", segment)

		require.Error(t, err)
		assert.Empty(t, cli.saveKeys, "Save must not be called when marshaling fails")
	})

	t.Run("valid segment is saved", func(t *testing.T) {
		cli := &saveRecordingKV{}
		segment := &datapb.SegmentInfo{
			ID:            1,
			CollectionID:  100,
			PartitionID:   10,
			InsertChannel: "ch-0",
		}

		err := writeRepairedSegment(cli, "by-dev/meta", segment)

		require.NoError(t, err)
		require.Len(t, cli.saveKeys, 1)
		assert.Equal(t, "by-dev/meta/datacoord-meta/s/100/10/1", cli.saveKeys[0])

		saved := &datapb.SegmentInfo{}
		require.NoError(t, proto.Unmarshal([]byte(cli.saveValues[0]), saved))
		assert.Equal(t, segment.GetInsertChannel(), saved.GetInsertChannel())
	})
}

type importJobRepairKV struct{ kv.MetaKV }

func importMarkerParams() *ImportJobParam {
	return &ImportJobParam{
		JobID: 692, CollectionID: 684, VChannels: []string{"ch_684v0"},
		RetainFor: "168h",
	}
}

func importMarkerCollection() *etcdpb.CollectionInfo {
	return &etcdpb.CollectionInfo{
		ID: 684, DbId: 2, PartitionIDs: []int64{685},
		VirtualChannelNames: []string{"ch_684v0", "ch_684v1"}, Schema: &schemapb.CollectionSchema{Name: "testondemand"},
	}
}

func TestCompletedImportJobMarker(t *testing.T) {
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	p := importMarkerParams()
	retention, err := validateImportJobRepair(p)
	require.NoError(t, err)
	coll := importMarkerCollection()
	job, err := completedImportJobMarker(p, coll, now, now.Add(retention))
	require.NoError(t, err)
	require.Equal(t, internalpb.ImportJobState_Completed, job.GetState())
	require.EqualValues(t, 5, job.GetState(), "Completed must match Kite's wire enum")
	require.EqualValues(t, 692, job.GetJobID())
	require.EqualValues(t, 2, job.GetDbID())
	require.Equal(t, "testondemand", job.GetCollectionName())
	require.Equal(t, p.VChannels, job.GetReadyVchannels())
	require.Equal(t, p.VChannels, job.GetCommittedVchannels())
	require.Equal(t, now.Format(time.RFC3339Nano), job.GetCompleteTime())
	require.Equal(t, now.Add(retention).UnixMilli(), int64(job.GetCleanupTs()>>18))
	require.Empty(t, job.GetFiles())
	require.Zero(t, job.GetRequestedDiskSize())

	pNow := importMarkerParams()
	pNow.VChannels = nil
	duration, err := validateImportJobRepair(pNow)
	require.NoError(t, err)
	inferred, err := completedImportJobMarker(pNow, coll, now, now.Add(duration))
	require.NoError(t, err)
	require.Equal(t, coll.VirtualChannelNames, inferred.Vchannels)
	require.Equal(t, inferred.Vchannels, inferred.ReadyVchannels)
	require.Equal(t, inferred.Vchannels, inferred.CommittedVchannels)
	require.Equal(t, now.Format(time.RFC3339Nano), inferred.CompleteTime)
	inferred.Vchannels[0] = "changed"
	require.Equal(t, "ch_684v0", coll.VirtualChannelNames[0])
	withoutChannels := importMarkerCollection()
	withoutChannels.VirtualChannelNames = nil
	_, err = completedImportJobMarker(pNow, withoutChannels, now, now)
	require.ErrorContains(t, err, "no vchannels")

	job.Schema.Name = "changed"
	job.PartitionIDs[0] = 999
	job.ReadyVchannels[0] = "changed"
	require.Equal(t, "testondemand", coll.Schema.Name)
	require.EqualValues(t, 685, coll.PartitionIDs[0])
	require.Equal(t, "ch_684v0", job.Vchannels[0])

	for _, change := range []func(*ImportJobParam){
		func(p *ImportJobParam) { p.JobID = 0 },
		func(p *ImportJobParam) { p.CollectionID = -1 },
		func(p *ImportJobParam) { p.RetainFor = "invalid" },
		func(p *ImportJobParam) { p.RetainFor = "0h" },
		func(p *ImportJobParam) { p.RetainFor = "8761h" },
	} {
		p := importMarkerParams()
		change(p)
		_, err := validateImportJobRepair(p)
		require.Error(t, err)
	}
	for _, change := range []func(*etcdpb.CollectionInfo){
		func(c *etcdpb.CollectionInfo) { c.ID++ },
		func(c *etcdpb.CollectionInfo) { c.State = etcdpb.CollectionState_CollectionDropped },
		func(c *etcdpb.CollectionInfo) { c.Schema = nil },
	} {
		coll := importMarkerCollection()
		change(coll)
		_, err := completedImportJobMarker(p, coll, now, now)
		require.Error(t, err)
	}
	for _, channels := range [][]string{{""}, {"wrong_684v0"}, {"ch_684v0", "ch_684v0"}} {
		p := importMarkerParams()
		p.VChannels = channels
		_, err := completedImportJobMarker(p, importMarkerCollection(), now, now)
		require.Error(t, err)
	}
}

func importRepairEtcd(t *testing.T) kv.MetaKV {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	u := url.URL{Scheme: "http", Host: "127.0.0.1:0"}
	cfg.ListenPeerUrls = []url.URL{u}
	cfg.ListenClientUrls = []url.URL{u}
	cfg.AdvertisePeerUrls = []url.URL{u}
	cfg.AdvertiseClientUrls = []url.URL{u}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(e.Close)
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		t.Fatal("isolated embedded etcd did not start")
	}
	cli := v3client.New(e.Server)
	t.Cleanup(func() { _ = cli.Close() })
	return kv.NewEtcdKV(cli)
}

func TestImportJobRepair_CreateOnlyAndReadback(t *testing.T) {
	raw := importRepairEtcd(t)
	file, err := os.CreateTemp(t.TempDir(), "audit-*.log")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	// Normal instance connections use an audit wrapper around the live etcd KV.
	cli := kv.NewFileAuditKV(raw, file)
	coll := models.NewCollection(importMarkerCollection(), "collection")
	m := mockey.Mock(common.GetCollectionByIDVersion).Return(coll, nil).Build()
	defer m.UnPatch()
	c := NewComponent(cli, nil, "isolated/meta")
	p := importMarkerParams()
	key := path.Join(c.basePath, common.ImportJobPrefix, "692")
	ctx := context.Background()
	// Exercise reflection-based command registration and the default dry-run flags.
	host := framework.NewCmdState("isolated", nil)
	root := host.GetCmd()
	host.MergeFunctionCommandsFrom(root, host, c)
	root.SetArgs([]string{"repair", "import-job", "--job", "692", "--collection", "684"})
	require.NoError(t, root.Execute())
	_, err = cli.Load(ctx, key)
	require.ErrorIs(t, err, kv.ErrKeyNotFound, "dry run must not write")
	require.Empty(t, importRepairAuditRecords(t, file), "dry run must not emit writes")
	before := time.Now()
	root.SetArgs([]string{"repair", "import-job", "--job", "692", "--collection", "684", "--run"})
	require.NoError(t, root.Execute())
	value, err := cli.Load(ctx, key)
	require.NoError(t, err)
	job := &datapb.ImportJob{}
	require.NoError(t, proto.Unmarshal([]byte(value), job))
	require.Equal(t, internalpb.ImportJobState_Completed, job.GetState())
	require.Equal(t, coll.GetProto().GetVirtualChannelNames(), job.GetCommittedVchannels())
	completed, err := time.Parse(time.RFC3339Nano, job.GetCompleteTime())
	require.NoError(t, err)
	require.False(t, completed.Before(before))
	require.False(t, completed.After(time.Now()))
	require.Equal(t, completed.Add(7*24*time.Hour).UnixMilli(), int64(job.GetCleanupTs()>>18), "default retention must be seven days from command start")
	require.ErrorContains(t, c.ImportJobCommand(ctx, p), "refusing to overwrite")
	after, err := cli.Load(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, after)
	records := importRepairAuditRecords(t, file)
	require.Len(t, records, 4)
	for i, op := range map[int]models.AuditOpType{0: models.AuditOpType_OpPut, 1: models.AuditOpType_OpPutBefore, 3: models.AuditOpType_OpPutAfter} {
		header := &models.AuditHeader{}
		require.NoError(t, proto.Unmarshal(records[i], header))
		require.EqualValues(t, op, header.OpType)
	}
	entry := &mvccpb.KeyValue{}
	require.NoError(t, proto.Unmarshal(records[2], protoadapt.MessageV2Of(entry)))
	require.Equal(t, key, string(entry.Key))
	require.Equal(t, value, string(entry.Value))
	require.ErrorContains(t, kv.CreateEtcdKeyIfAbsent(ctx, cli, key, "overwrite"), "refusing to overwrite")
	require.Len(t, importRepairAuditRecords(t, file), 6, "failed create must not record a successful put")
	after, err = cli.Load(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, after)

	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, value := range []string{"first", "second"} {
		wg.Add(1)
		go func(value string) {
			defer wg.Done()
			results <- kv.CreateEtcdKeyIfAbsent(ctx, raw, "race/key", value)
		}(value)
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else {
			require.ErrorContains(t, err, "refusing to overwrite")
		}
	}
	require.Equal(t, 1, winners)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, kv.CreateEtcdKeyIfAbsent(canceled, cli, "cancel/key", "value"))
	require.ErrorContains(t, kv.CreateEtcdKeyIfAbsent(ctx, &importJobRepairKV{}, "k", "v"), "live etcd")
}

func TestImportJobRepair_ErrorBoundaries(t *testing.T) {
	cli := &importJobRepairKV{}
	c := NewComponent(cli, nil, "isolated/meta")
	ctx := context.Background()
	failure := errors.New("injected dependency failure")
	for _, scenario := range []string{"invalid", "load", "collection", "wrong-channel", "marshal", "preview", "create", "readback", "mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			p := importMarkerParams()
			p.ExecutionParam = framework.ExecutionParam{Run: true}
			coll := models.NewCollection(importMarkerCollection(), "collection")
			loads := 0
			load := mockey.Mock((*importJobRepairKV).Load).To(func(_ *importJobRepairKV, _ context.Context, _ string, _ ...kv.LoadOption) (string, error) {
				loads++
				if scenario == "load" || loads > 1 && scenario == "readback" {
					return "", failure
				}
				if loads > 1 {
					return "unexpected value", nil
				}
				return "", kv.ErrKeyNotFound
			}).Build()
			defer load.UnPatch()
			collectionErr := error(nil)
			if scenario == "collection" {
				collectionErr = failure
			}
			collection := mockey.Mock(common.GetCollectionByIDVersion).Return(coll, collectionErr).Build()
			defer collection.UnPatch()
			createErr := error(nil)
			if scenario == "create" {
				createErr = failure
			}
			create := mockey.Mock(kv.CreateEtcdKeyIfAbsent).Return(createErr).Build()
			defer create.UnPatch()
			switch scenario {
			case "invalid":
				p.JobID = 0
			case "wrong-channel":
				p.VChannels = []string{"other"}
			case "marshal":
				coll.GetProto().Schema.Name = "\xff"
			case "preview":
				preview := mockey.MockValue(&renderImportJobPreview).To(func(proto.Message) ([]byte, error) {
					return nil, failure
				})
				defer preview.UnPatch()
			}
			err := c.ImportJobCommand(ctx, p)
			require.Error(t, err)
			if scenario == "preview" {
				require.ErrorContains(t, err, "render import job preview")
				require.ErrorIs(t, err, failure)
			}
			if scenario != "readback" && scenario != "mismatch" && scenario != "create" {
				require.Zero(t, create.Times(), "validation and preview errors must not write")
			}
		})
	}
}

func importRepairAuditRecords(t *testing.T, file *os.File) [][]byte {
	t.Helper()
	data, err := os.ReadFile(file.Name())
	require.NoError(t, err)
	reader := bytes.NewReader(data)
	var records [][]byte
	for reader.Len() > 0 {
		var size uint64
		require.NoError(t, binary.Read(reader, binary.LittleEndian, &size))
		require.LessOrEqual(t, size, uint64(reader.Len()))
		record := make([]byte, size)
		_, err := io.ReadFull(reader, record)
		require.NoError(t, err)
		records = append(records, record)
	}
	return records
}

func segmentTypeFixture(t *testing.T) (*SegmentIndexTypeParam, map[string]string) {
	t.Helper()
	p := &SegmentIndexTypeParam{CollectionID: 100, IndexID: 200, IndexType: "HNSW"}
	values := make(map[string]string)
	put := func(key string, message proto.Message) {
		value, err := proto.Marshal(message)
		require.NoError(t, err)
		values[key] = string(value)
	}
	put("meta/field-index/100/200", &indexpb.FieldIndex{IndexInfo: &indexpb.IndexInfo{CollectionID: 100, IndexID: 200, FieldID: 300, IndexName: "custom"}})
	for _, id := range []int64{1, 2} {
		put(fmt.Sprintf("meta/datacoord-meta/s/100/10/%d", id), &datapb.SegmentInfo{ID: id, CollectionID: 100, PartitionID: 10, State: commonpb.SegmentState_Flushed})
		index := &indexpb.SegmentIndex{
			CollectionID: 100, PartitionID: 10, SegmentID: id, IndexID: 200, BuildID: 1000 + id,
			State: commonpb.IndexState_Finished, IndexFileKeys: []string{"index"}, IndexVersion: 7, NumRows: 10000, SerializeSize: 1024,
		}
		// Newer Milvus fields must survive a repair performed by an older Birdwatcher.
		index.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 42))
		put(fmt.Sprintf("meta/segment-index/100/10/%d/%d", id, 1000+id), index)
	}
	return p, values
}

func segmentTypeSnapshot(values map[string]string) *kv.EtcdRepairSnapshot {
	s := &kv.EtcdRepairSnapshot{Entries: make(map[string]*mvccpb.KeyValue)}
	for key, value := range values {
		s.Entries[key] = &mvccpb.KeyValue{Key: []byte(key), Value: []byte(value)}
	}
	return s
}

func segmentTypePlan(s *kv.EtcdRepairSnapshot, p *SegmentIndexTypeParam) (map[string][]string, map[string]string, string, error) {
	return planSegmentIndexTypeRepair(s, p, "meta/field-index/100/", "meta/datacoord-meta/s/100/", "meta/segment-index/100/")
}

func TestSegmentIndexTypePlan(t *testing.T) {
	p, values := segmentTypeFixture(t)
	s := segmentTypeSnapshot(values)
	dependencies, updates, preview, err := segmentTypePlan(s, p)
	require.NoError(t, err)
	require.Len(t, dependencies, 2)
	require.Len(t, updates, 2)
	require.Contains(t, preview, `type="" -> "HNSW"`)
	for key, value := range updates {
		before, after := &indexpb.SegmentIndex{}, &indexpb.SegmentIndex{}
		require.NoError(t, proto.Unmarshal([]byte(values[key]), before))
		require.NoError(t, proto.Unmarshal([]byte(value), after))
		require.Equal(t, "HNSW", after.IndexType)
		after.IndexType = before.IndexType
		require.True(t, proto.Equal(before, after), "all other metadata and unknown fields must survive")
	}
	p.SegmentID = 1
	_, updates, _, err = segmentTypePlan(s, p)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	for key, value := range updates {
		s.Entries[key].Value = []byte(value)
	}
	_, updates, preview, err = segmentTypePlan(s, p)
	require.NoError(t, err)
	require.Empty(t, updates, "same type is idempotent")
	require.Empty(t, preview, "already-correct records are omitted from the preview")
	p.SegmentID = 0
	_, updates, preview, err = segmentTypePlan(s, p)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	require.Equal(t, "segment=2 index=200 build=1002 type=\"\" -> \"HNSW\"\n", preview,
		"mixed plans preview only the record that would change")
	p.SegmentID = 1
	p.IndexType = "DISKANN"
	_, updates, _, err = segmentTypePlan(s, p)
	require.NoError(t, err)
	require.Len(t, updates, 1, "caller can correct a nonempty but wrong type")
}

func TestSegmentIndexTypePlan_Rejections(t *testing.T) {
	for _, scenario := range []string{"missing-index", "bad-definition", "deleted-index", "wrong-index", "bad-segment", "wrong-segment", "partition-mismatch", "missing-segment", "not-flushed", "manifest", "bad-build", "wrong-build", "manifest-layout", "unknown-layout", "no-files", "deleted-build", "in-progress", "dropped", "absent-selection", "other-index"} {
		t.Run(scenario, func(t *testing.T) {
			p, values := segmentTypeFixture(t)
			p.SegmentID = 2
			segmentKey, buildKey, indexKey := "meta/datacoord-meta/s/100/10/2", "meta/segment-index/100/10/2/1002", "meta/field-index/100/200"
			segment, build, definition := &datapb.SegmentInfo{}, &indexpb.SegmentIndex{}, &indexpb.FieldIndex{}
			require.NoError(t, proto.Unmarshal([]byte(values[segmentKey]), segment))
			require.NoError(t, proto.Unmarshal([]byte(values[buildKey]), build))
			require.NoError(t, proto.Unmarshal([]byte(values[indexKey]), definition))
			switch scenario {
			case "deleted-index":
				definition.Deleted = true
			case "wrong-index":
				definition.IndexInfo.IndexID++
			case "wrong-segment":
				segment.ID++
			case "partition-mismatch":
				segment.PartitionID = 11
			case "not-flushed":
				segment.State = commonpb.SegmentState_Importing
			case "manifest":
				segment.ManifestHasIndex = true
			case "wrong-build":
				build.CollectionID++
			case "manifest-layout":
				build.IndexStorePathVersion = 2
			case "unknown-layout":
				build.IndexStorePathVersion = 3
			case "no-files":
				build.IndexFileKeys = nil
			case "deleted-build":
				build.Deleted = true
			case "in-progress":
				build.State = commonpb.IndexState_InProgress
			case "dropped":
				segment.State = commonpb.SegmentState_Dropped
			case "absent-selection":
				p.SegmentID = 999
			case "other-index":
				build.IndexID++
			}
			for key, message := range map[string]proto.Message{segmentKey: segment, buildKey: build, indexKey: definition} {
				value, err := proto.Marshal(message)
				require.NoError(t, err)
				values[key] = string(value)
			}
			switch scenario {
			case "missing-index":
				delete(values, indexKey)
			case "missing-segment":
				delete(values, segmentKey)
				p.SegmentID = 0
			case "partition-mismatch":
				values["meta/datacoord-meta/s/100/11/2"] = values[segmentKey]
				delete(values, segmentKey)
			case "bad-definition":
				values[indexKey] = "\xff"
			case "bad-segment":
				values[segmentKey] = "\xff"
			case "bad-build":
				values[buildKey] = "\xff"
			}
			_, updates, _, err := segmentTypePlan(segmentTypeSnapshot(values), p)
			require.Error(t, err)
			require.Empty(t, updates, "never return a partial plan")
		})
	}
}

func TestSegmentIndexTypeCommand_PerRecordCASAndPreview(t *testing.T) {
	ctx := context.Background()
	raw := importRepairEtcd(t)
	p, values := segmentTypeFixture(t)
	for key, value := range values {
		require.NoError(t, raw.Save(ctx, key, value))
	}
	file, err := os.CreateTemp(t.TempDir(), "audit")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	cli := kv.NewFileAuditKV(raw, file)
	c := NewComponent(cli, nil, "meta")
	host := framework.NewCmdState("isolated", nil)
	root := host.GetCmd()
	host.MergeFunctionCommandsFrom(root, host, c)
	args := []string{"repair", "segment-index-type", "--collection", "100", "--index", "200", "--index-type", "HNSW"}
	root.SetArgs(args)
	require.NoError(t, root.Execute())
	for key, expected := range values {
		value, err := raw.Load(ctx, key)
		require.NoError(t, err)
		require.Equal(t, expected, value)
	}
	require.Empty(t, importRepairAuditRecords(t, file))
	root.SetArgs(append(slices.Clone(args), "--run"))
	require.NoError(t, root.Execute())
	response, err := kv.MustGetETCDClient(raw).Get(ctx, "meta/segment-index/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, response.Kvs, 2)
	require.NotEqual(t, response.Kvs[0].ModRevision, response.Kvs[1].ModRevision, "each record commits independently")
	for _, entry := range response.Kvs {
		actual, original := &indexpb.SegmentIndex{}, &indexpb.SegmentIndex{}
		require.NoError(t, proto.Unmarshal(entry.Value, actual))
		require.Equal(t, "HNSW", actual.IndexType)
		require.NoError(t, proto.Unmarshal([]byte(values[string(entry.Key)]), original))
		actual.IndexType = original.IndexType
		require.True(t, proto.Equal(actual, original))
	}
	records := importRepairAuditRecords(t, file)
	require.Len(t, records, 8, "audit includes two independently committed puts")
	p.Run = true
	require.NoError(t, c.SegmentIndexTypeCommand(ctx, p))
	require.Len(t, importRepairAuditRecords(t, file), len(records), "idempotent run does not write")
	for _, change := range []func(*SegmentIndexTypeParam){
		func(p *SegmentIndexTypeParam) { p.CollectionID = 0 }, func(p *SegmentIndexTypeParam) { p.IndexID = 0 },
		func(p *SegmentIndexTypeParam) { p.SegmentID = -1 }, func(p *SegmentIndexTypeParam) { p.IndexType = "" },
		func(p *SegmentIndexTypeParam) { p.IndexType = "AUTOINDEX" }, func(p *SegmentIndexTypeParam) { p.IndexType = "hnsw" },
		func(p *SegmentIndexTypeParam) { p.IndexType = "HNSW\n" },
	} {
		bad := *p
		change(&bad)
		require.Error(t, c.SegmentIndexTypeCommand(ctx, &bad))
	}
	require.Len(t, importRepairAuditRecords(t, file), len(records))
}

func TestSegmentIndexTypeCommand_ManifestRejectsWholeScope(t *testing.T) {
	ctx := context.Background()
	cli := importRepairEtcd(t)
	p, values := segmentTypeFixture(t)
	p.Run = true
	// No etcd index row exists for segment 2; the inventory must still reject it.
	delete(values, "meta/segment-index/100/10/2/1002")
	seg := &datapb.SegmentInfo{}
	key := "meta/datacoord-meta/s/100/10/2"
	require.NoError(t, proto.Unmarshal([]byte(values[key]), seg))
	seg.ManifestHasIndex = true
	value, err := proto.Marshal(seg)
	require.NoError(t, err)
	values[key] = string(value)
	for key, value := range values {
		require.NoError(t, cli.Save(ctx, key, value))
	}
	c := NewComponent(cli, nil, "meta")
	require.ErrorContains(t, c.SegmentIndexTypeCommand(ctx, p), "manifest indexes")
	for key, expected := range values {
		value, err := cli.Load(ctx, key)
		require.NoError(t, err)
		require.Equal(t, expected, value)
	}
	p.SegmentID = 1
	require.NoError(t, c.SegmentIndexTypeCommand(ctx, p), "explicit segment scope excludes manifest-only segment")
}

func TestSegmentIndexTypeCAS(t *testing.T) {
	ctx := context.Background()
	cli := importRepairEtcd(t)
	for _, scenario := range []string{"modified", "deleted", "segment-changed", "definition-changed", "delete-recreate", "missing-dependency", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			require.NoError(t, cli.RemoveWithPrefix(ctx, "meta/"))
			p, values := segmentTypeFixture(t)
			for key, value := range values {
				require.NoError(t, cli.Save(ctx, key, value))
			}
			snapshot, err := kv.ReadEtcdRepairSnapshot(ctx, cli, []string{"meta/field-index/100/", "meta/datacoord-meta/s/100/", "meta/segment-index/100/"})
			require.NoError(t, err)
			deps, updates, _, err := segmentTypePlan(snapshot, p)
			require.NoError(t, err)
			key := "meta/segment-index/100/10/1/1001"
			original := values[key]
			applyCtx := ctx
			switch scenario {
			case "modified":
				require.NoError(t, cli.Save(ctx, key, "changed"))
			case "deleted":
				require.NoError(t, cli.Remove(ctx, key))
			case "segment-changed":
				require.NoError(t, cli.Save(ctx, "meta/datacoord-meta/s/100/10/1", "changed"))
			case "definition-changed":
				require.NoError(t, cli.Save(ctx, "meta/field-index/100/200", "changed"))
			case "delete-recreate":
				require.NoError(t, cli.Remove(ctx, key))
				require.NoError(t, cli.Save(ctx, key, original))
			case "missing-dependency":
				deps[key] = append(deps[key], "missing")
			case "canceled":
				var cancel context.CancelFunc
				applyCtx, cancel = context.WithCancel(ctx)
				cancel()
			}
			committed, err := kv.ApplyEtcdRepair(applyCtx, cli, snapshot, deps, updates)
			require.Zero(t, committed)
			require.Error(t, err)
			second := "meta/segment-index/100/10/2/1002"
			stored, err := cli.Load(ctx, second)
			require.NoError(t, err)
			require.Equal(t, values[second], stored, "a conflict on the first record stops before later writes")
		})
	}
	// More records than fit in one transaction still succeed with per-record CAS.
	updates := make(map[string]string)
	for i := 0; i < 130; i++ {
		key := fmt.Sprintf("large/%d", i)
		require.NoError(t, cli.Save(ctx, key, "old"))
		updates[key] = "new"
	}
	snapshot, err := kv.ReadEtcdRepairSnapshot(ctx, cli, []string{"large/"})
	require.NoError(t, err)
	committed, err := kv.ApplyEtcdRepair(ctx, cli, snapshot, nil, updates)
	require.NoError(t, err)
	require.Equal(t, 130, committed)
	_, stored, err := cli.LoadWithPrefix(ctx, "large/")
	require.NoError(t, err)
	for _, value := range stored {
		require.Equal(t, "new", value)
	}
	_, err = kv.ReadEtcdRepairSnapshot(ctx, &importJobRepairKV{}, []string{"meta/"})
	require.ErrorContains(t, err, "live etcd")
	_, err = kv.ApplyEtcdRepair(ctx, &importJobRepairKV{}, snapshot, nil, updates)
	require.ErrorContains(t, err, "live etcd")
	committed, err = kv.ApplyEtcdRepair(ctx, cli, snapshot, nil, nil)
	require.NoError(t, err)
	require.Zero(t, committed)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = kv.ReadEtcdRepairSnapshot(canceled, cli, []string{"meta/"})
	require.Error(t, err)
}

func TestSegmentIndexTypePlan_RemainingBoundaries(t *testing.T) {
	p, values := segmentTypeFixture(t)
	seg := &datapb.SegmentInfo{}
	key := "meta/datacoord-meta/s/100/10/2"
	require.NoError(t, proto.Unmarshal([]byte(values[key]), seg))
	seg.State = commonpb.SegmentState_Dropped
	value, err := proto.Marshal(seg)
	require.NoError(t, err)
	values[key] = string(value)
	_, updates, _, err := segmentTypePlan(segmentTypeSnapshot(values), p)
	require.NoError(t, err)
	require.Len(t, updates, 1)

	p, values = segmentTypeFixture(t)
	duplicate := &datapb.SegmentInfo{CollectionID: 100, PartitionID: 11, ID: 2, State: commonpb.SegmentState_Flushed}
	value, err = proto.Marshal(duplicate)
	require.NoError(t, err)
	values["meta/datacoord-meta/s/100/11/2"] = string(value)
	_, _, _, err = segmentTypePlan(segmentTypeSnapshot(values), p)
	require.ErrorContains(t, err, "duplicate segment")

	p, values = segmentTypeFixture(t)
	failure := errors.New("injected serialization failure")
	m := mockey.Mock(proto.Marshal).Return([]byte(nil), failure).Build()
	defer m.UnPatch()
	_, updates, _, err = segmentTypePlan(segmentTypeSnapshot(values), p)
	require.ErrorIs(t, err, failure)
	require.Empty(t, updates)
}

func TestSegmentIndexTypeCommand_ErrorBoundaries(t *testing.T) {
	ctx := context.Background()
	cli := importRepairEtcd(t)
	p, values := segmentTypeFixture(t)
	for key, value := range values {
		require.NoError(t, cli.Save(ctx, key, value))
	}
	c := NewComponent(cli, nil, "meta")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, c.SegmentIndexTypeCommand(canceled, p))
	p.Run = true
	failure := errors.New("injected CAS failure")
	m := mockey.Mock(kv.ApplyEtcdRepair).Return(1, failure).Build()
	defer m.UnPatch()
	err := c.SegmentIndexTypeCommand(ctx, p)
	require.ErrorIs(t, err, failure)
	require.ErrorContains(t, err, "1/2 confirmed commits")
	for key, expected := range values {
		value, err := cli.Load(ctx, key)
		require.NoError(t, err)
		require.Equal(t, expected, value)
	}
}

func TestSegmentIndexTypeCAS_ReadbackFailure(t *testing.T) {
	ctx := context.Background()
	cli := importRepairEtcd(t)
	for _, scenario := range []string{"read-error", "missing", "different"} {
		t.Run(scenario, func(t *testing.T) {
			require.NoError(t, cli.Save(ctx, "readback/key", "old"))
			snapshot, err := kv.ReadEtcdRepairSnapshot(ctx, cli, []string{"readback/"})
			require.NoError(t, err)
			read := &clientv3.GetResponse{}
			if scenario == "different" {
				read.Kvs = []*mvccpb.KeyValue{{Key: []byte("readback/key"), Value: []byte("concurrent update")}}
			}
			var readErr error
			if scenario == "read-error" {
				readErr = errors.New("readback unavailable")
			}
			// Only the post-commit readback fails; the actual write transaction runs.
			m := mockey.Mock(mockey.GetMethod(kv.MustGetETCDClient(cli).KV, "Get")).Return(read, readErr).Build()
			committed, err := kv.ApplyEtcdRepair(ctx, cli, snapshot, nil, map[string]string{"readback/key": "new"})
			require.Equal(t, 1, committed)
			m.UnPatch()
			require.ErrorContains(t, err, "repair committed but readback")
			stored, err := cli.Load(ctx, "readback/key")
			require.NoError(t, err)
			require.Equal(t, "new", stored, "readback failure must not trigger a rollback or duplicate write")
		})
	}
}

func TestSegmentIndexTypeCAS_PartialFailureAndResume(t *testing.T) {
	ctx := context.Background()
	cli := importRepairEtcd(t)
	p, values := segmentTypeFixture(t)
	for key, value := range values {
		require.NoError(t, cli.Save(ctx, key, value))
	}
	snapshot, err := kv.ReadEtcdRepairSnapshot(ctx, cli, []string{"meta/field-index/100/", "meta/datacoord-meta/s/100/", "meta/segment-index/100/"})
	require.NoError(t, err)
	dependencies, updates, _, err := segmentTypePlan(snapshot, p)
	require.NoError(t, err)
	first, second := "meta/segment-index/100/10/1/1001", "meta/segment-index/100/10/2/1002"
	// Another writer changes only the second record before this repair reaches it.
	changed := &indexpb.SegmentIndex{}
	require.NoError(t, proto.Unmarshal([]byte(values[second]), changed))
	changed.SerializeSize++
	value, err := proto.Marshal(changed)
	require.NoError(t, err)
	require.NoError(t, cli.Save(ctx, second, string(value)))
	committed, err := kv.ApplyEtcdRepair(ctx, cli, snapshot, dependencies, updates)
	require.ErrorContains(t, err, "metadata changed")
	require.Equal(t, 1, committed)
	stored, err := cli.Load(ctx, first)
	require.NoError(t, err)
	require.Equal(t, updates[first], stored)
	stored, err = cli.Load(ctx, second)
	require.NoError(t, err)
	require.Equal(t, string(value), stored, "conflicting record is not overwritten")
	before, err := kv.MustGetETCDClient(cli).Get(ctx, first)
	require.NoError(t, err)
	p.Run = true
	require.NoError(t, NewComponent(cli, nil, "meta").SegmentIndexTypeCommand(ctx, p))
	after, err := kv.MustGetETCDClient(cli).Get(ctx, first)
	require.NoError(t, err)
	require.Equal(t, before.Kvs[0].ModRevision, after.Kvs[0].ModRevision, "resume skips already repaired records")
	stored, err = cli.Load(ctx, second)
	require.NoError(t, err)
	require.NoError(t, proto.Unmarshal([]byte(stored), changed))
	require.Equal(t, "HNSW", changed.IndexType)
	require.EqualValues(t, 1025, changed.SerializeSize, "fresh plan preserves the concurrent change")
}
