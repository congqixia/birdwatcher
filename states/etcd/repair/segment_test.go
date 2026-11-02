package repair

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/url"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
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
