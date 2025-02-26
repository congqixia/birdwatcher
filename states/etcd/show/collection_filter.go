package show

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/samber/lo"
)

type CollectionFilterParam struct {
	framework.ParamBase `use:"show collections-filter" desc:"list collection with criteria met"`
}

func (c *ComponentShow) CollectionFilterCommand(ctx context.Context, p *CollectionFilterParam) error {
	collections, err := common.ListCollectionsVersion(ctx, c.client, c.metaPath, etcdversion.GetVersion())

	if err != nil {
		return err
	}

	id2name := lo.SliceToMap(collections, func(c *models.Collection) (int64, string) {
		return c.ID, c.Schema.Name
	})

	fieldIndexes, err := c.listIndexMetaV2(ctx)
	if err != nil {
		return err
	}

LOOP:
	for _, index := range fieldIndexes {
		for _, kv := range index.IndexInfo.TypeParams {
			if kv.Key == "index_type" && (kv.Value == "INVERTED" || kv.Value == "BITMAP") {
				fmt.Println("legacy index found for collection [%d]%s", index.IndexInfo.CollectionID, id2name[index.IndexInfo.CollectionID])
				continue LOOP
			}
		}
	}
	return nil
}
