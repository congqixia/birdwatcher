package repair

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

var renderImportJobPreview = protojson.MarshalOptions{Indent: "  "}.Marshal

type ImportJobParam struct {
	framework.ExecutionParam `use:"repair import-job" desc:"recreate a GCed, proven-completed import job as a terminal marker for CommitImport replay; writes require live etcd (audit wrappers supported; TiKV unsupported). Verify target ImportJob metadata and CommitImport replay compatibility before use"`
	JobID                    int64    `name:"job" default:"0" desc:"original completed import job ID"`
	CollectionID             int64    `name:"collection" default:"0" desc:"original collection ID; must still exist"`
	VChannels                []string `name:"vchannel" desc:"override marker channels; defaults to all current collection vchannels"`
	RetainFor                string   `name:"retain-for" default:"168h" desc:"retain marker for this duration from now; keep until the replay bug is fixed"`
}

func (c *ComponentRepair) ImportJobCommand(ctx context.Context, p *ImportJobParam) error {
	now := time.Now()
	retention, err := validateImportJobRepair(p)
	if err != nil {
		return err
	}
	key := path.Join(c.basePath, common.ImportJobPrefix, strconv.FormatInt(p.JobID, 10))
	if _, err := c.client.Load(ctx, key); !errors.Is(err, kv.ErrKeyNotFound) {
		if err != nil {
			return errors.Wrap(err, "check existing import job")
		}
		return errors.Newf("import job key %s already exists; refusing to overwrite", key)
	}
	collection, err := common.GetCollectionByIDVersion(ctx, c.client, c.basePath, p.CollectionID)
	if err != nil {
		return errors.Wrap(err, "load original collection")
	}
	job, err := completedImportJobMarker(p, collection.GetProto(), now, now.Add(retention))
	if err != nil {
		return err
	}
	value, err := proto.Marshal(job)
	if err != nil {
		return errors.Wrap(err, "marshal import job")
	}
	// Encoding succeeds before any write; JSON is a preview, not the stored format.
	detail, err := renderImportJobPreview(job)
	if err != nil {
		return errors.Wrap(err, "render import job preview")
	}
	fmt.Println("WARNING: Verify the target uses compatible ImportJob metadata, Completed state (wire value 5), channel completion and cleanup timestamp semantics. Target compatibility is not detected automatically.")
	fmt.Printf("Key: %s\nCompleted terminal marker (original tasks/files are not restored):\n%s\n", key, detail)
	if !p.Run {
		fmt.Println("dry run, nothing written. Re-run with --run to create the missing key.")
		return nil
	}
	if err := kv.CreateEtcdKeyIfAbsent(ctx, c.client, key, string(value)); err != nil {
		return err
	}
	stored, err := c.client.Load(ctx, key)
	if err != nil {
		return errors.Wrap(err, "key was created, but readback failed; inspect it before retrying")
	}
	if stored != string(value) {
		return errors.Newf("key %s was created, but readback differs; inspect it before restarting", key)
	}
	fmt.Println("Completed marker created and readback verified. Restart Kite Coordinator to load it into memory.")
	fmt.Println("Verify CommitImport retries stop and checkpoints advance. Keep the marker until the replay bug is fixed.")
	return nil
}

func validateImportJobRepair(p *ImportJobParam) (time.Duration, error) {
	if p.JobID <= 0 || p.CollectionID <= 0 {
		return 0, errors.New("positive --job and --collection are required")
	}
	retention, err := time.ParseDuration(p.RetainFor)
	if err != nil || retention < time.Hour || retention > 365*24*time.Hour {
		return 0, errors.New("--retain-for must be between 1h and 8760h")
	}
	return retention, nil
}

func completedImportJobMarker(p *ImportJobParam, collection *etcdpb.CollectionInfo, completed, cleanup time.Time) (*datapb.ImportJob, error) {
	if collection.GetID() != p.CollectionID || collection.GetState() != etcdpb.CollectionState_CollectionCreated || collection.GetSchema() == nil {
		return nil, errors.New("original collection must be live and have a schema")
	}
	sourceChannels := p.VChannels
	if len(sourceChannels) == 0 {
		sourceChannels = collection.GetVirtualChannelNames()
	}
	if len(sourceChannels) == 0 {
		return nil, errors.New("original collection has no vchannels")
	}
	channels := make([]string, 0, len(sourceChannels))
	for _, channel := range sourceChannels {
		if strings.TrimSpace(channel) == "" || !slices.Contains(collection.GetVirtualChannelNames(), channel) || slices.Contains(channels, channel) {
			return nil, errors.Newf("vchannel %q is not in the collection or was specified more than once", channel)
		}
		channels = append(channels, channel)
	}
	return &datapb.ImportJob{
		JobID:              p.JobID,
		DbID:               collection.GetDbId(),
		CollectionID:       collection.GetID(),
		CollectionName:     collection.GetSchema().GetName(),
		PartitionIDs:       slices.Clone(collection.GetPartitionIDs()),
		Schema:             proto.Clone(collection.GetSchema()).(*schemapb.CollectionSchema),
		Vchannels:          channels,
		ReadyVchannels:     slices.Clone(channels),
		CommittedVchannels: slices.Clone(channels),
		State:              internalpb.ImportJobState_Completed,
		CompleteTime:       completed.UTC().Format(time.RFC3339Nano),
		CleanupTs:          uint64(cleanup.UnixMilli()) << 18,
		Reason:             "Temporary completed import marker reconstructed by Birdwatcher",
	}, nil
}
