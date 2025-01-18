package remove

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

type RemoveCollectionParam struct {
	framework.ParamBase `use:"remove collection" desc:"Remove collection & channel meta for collection"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to remove"`
	Run                 bool  `name:"run" default:"false" desc:"actual remove session, default in dry-run mode"`
}

func (c *ComponentRemove) RemoveCollectionCommand(ctx context.Context, p *RemoveCollectionParam) error {
	if p.CollectionID == 0 {
		return errors.New("collection id must be provided!")
	}

	collection, err := common.GetCollectionByIDVersion(context.Background(), c.client, c.basePath, etcdversion.GetVersion(), p.CollectionID)
	if err != nil {
		return err
	}

	cleanCollectionDropMeta(c.client, c.basePath, collection, p.Run)
	return nil
}
