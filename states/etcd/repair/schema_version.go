package repair

import (
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// SchemaVersionParam is the parameter of `repair schema-version`.
// It overwrites the same schema version field (schemapb.CollectionSchema.Version)
// to a given value in two places: the rootcoord collection meta
// (etcdpb.CollectionInfo.Schema.Version) and every schema snapshot of the
// collection kept in the streamingnode WAL recovery storage
// (streamingpb.CollectionSchemaOfVChannel.Schema.Version).
type SchemaVersionParam struct {
	framework.ExecutionParam `use:"repair schema-version" desc:"overwrite the collection schema version to a given value in both the rootcoord collection meta (etcdpb.CollectionInfo.Schema.Version) and the streamingnode WAL recovery storage (streamingpb.CollectionSchemaOfVChannel.Schema.Version)"`
	CollectionID             int64  `name:"collectionID" default:"0" desc:"collection id to repair"`
	Version                  int64  `name:"version" default:"0" desc:"the schema version to write, must be in [0, 2147483647]"`
	Scope                    string `name:"scope" default:"latest" desc:"which WAL recovery schema snapshots to repair: latest (only the newest non-dropped snapshot per vchannel, default) or all" values:"latest,all"`
}

// selectSchemaSnapshots returns the schema snapshots to repair for one vchannel
// according to the given scope. "latest" keeps only the snapshot with the
// largest checkpoint time tick among non-dropped entries, mirroring the latest
// schema selection of the streamingnode recovery module.
func selectSchemaSnapshots(schemas []*streamingpb.CollectionSchemaOfVChannel, scope string) []*streamingpb.CollectionSchemaOfVChannel {
	if scope != "latest" {
		return schemas
	}
	var latest *streamingpb.CollectionSchemaOfVChannel
	for _, schema := range schemas {
		if schema.GetState() == streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED {
			continue
		}
		if latest == nil || schema.GetCheckpointTimeTick() > latest.GetCheckpointTimeTick() {
			latest = schema
		}
	}
	if latest == nil {
		return nil
	}
	return []*streamingpb.CollectionSchemaOfVChannel{latest}
}

// SchemaVersionCommand returns the `repair schema-version` command.
func (c *ComponentRepair) SchemaVersionCommand(ctx context.Context, p *SchemaVersionParam) error {
	if p.CollectionID <= 0 {
		return errors.New("collectionID must be specified")
	}
	if p.Version < 0 || p.Version > int64(math.MaxInt32) {
		return errors.Errorf("version %d is out of range, must be in [0, %d]", p.Version, int64(math.MaxInt32))
	}
	if p.Scope == "" {
		// The framework injects the default from the struct tag, but direct
		// callers may leave it empty; treat it as the default "latest".
		p.Scope = "latest"
	}
	if p.Scope != "latest" && p.Scope != "all" {
		return errors.Errorf("scope %q is invalid, must be latest or all", p.Scope)
	}
	target := int32(p.Version)

	// 1. etcdpb.CollectionInfo: overwrite Schema.Version of the collection meta.
	// Note: we intentionally do NOT enrich the collection with its separately
	// stored fields (root-coord/fields/{id}), otherwise those fields would be
	// written back into the collection-info key as well.
	collections, err := common.ListCollectionWithoutFieldsBy(ctx, c.client, c.basePath, common.CollectionSelector{CollectionID: p.CollectionID})
	if err != nil {
		return errors.Wrap(err, "list collections")
	}
	if len(collections) == 0 {
		return errors.Wrap(common.ErrCollectionNotFound, fmt.Sprintf("collection %d not found", p.CollectionID))
	}
	for _, coll := range collections {
		info := coll.GetProto()
		if info.GetSchema() == nil {
			return errors.Errorf("collection %d has empty schema, refuse to write", p.CollectionID)
		}
		before := info.GetSchema().GetVersion()
		fmt.Printf("[etcd collection meta] %s\n  schema version: %d -> %d\n", coll.Key(), before, target)
		if !p.IsDryRun() {
			clone := proto.Clone(info).(*etcdpb.CollectionInfo)
			clone.Schema.Version = target
			bs, err := proto.Marshal(clone)
			if err != nil {
				return errors.Wrap(err, "marshal collection info")
			}
			if err := c.client.Save(ctx, coll.Key(), string(bs)); err != nil {
				return errors.Wrapf(err, "save collection info at %s", coll.Key())
			}
		}
	}

	// 2. streamingpb.CollectionSchemaOfVChannel: overwrite Schema.Version of every
	// schema snapshot the collection has in the WAL recovery storage.
	pchannels, err := common.ListWALDistribution(ctx, c.client, c.basePath, "")
	if err != nil {
		return errors.Wrap(err, "list wal distribution")
	}
	visited := map[string]struct{}{}
	schemaCount := 0
	for _, pchannelMeta := range pchannels {
		pchannel := pchannelMeta.GetChannel().GetName()
		if _, ok := visited[pchannel]; ok {
			continue
		}
		visited[pchannel] = struct{}{}

		rs, err := common.ListWALRecoveryStorage(ctx, c.client, c.basePath, pchannel)
		if err != nil {
			continue // pchannel without recovery storage is not ours
		}
		for _, vc := range rs.VChannels {
			if vc.GetCollectionInfo().GetCollectionId() != p.CollectionID {
				continue
			}
			for _, schema := range selectSchemaSnapshots(vc.GetCollectionInfo().GetSchemas(), p.Scope) {
				before := schema.GetSchema().GetVersion()
				fmt.Printf("[streamingnode wal recovery] %s/vchannel/%s/schema/%d\n  schema version: %d -> %d\n",
					pchannel, vc.GetVchannel(), schema.GetCheckpointTimeTick(), before, target)
				schemaCount++
				if !p.IsDryRun() {
					clone := proto.Clone(schema).(*streamingpb.CollectionSchemaOfVChannel)
					clone.Schema.Version = target
					if err := common.UpdateSchemaForVChannel(ctx, c.client, c.basePath, vc, clone); err != nil {
						return errors.Wrapf(err, "save vchannel schema of %s", vc.GetVchannel())
					}
				}
			}
		}
	}

	if schemaCount == 0 {
		fmt.Println("no schema snapshot found for this collection in the WAL recovery storage")
	}

	if p.IsDryRun() {
		fmt.Println("dry run, nothing written. re-run with --run to apply.")
		return nil
	}
	fmt.Println("schema version repaired successfully.")
	return nil
}
