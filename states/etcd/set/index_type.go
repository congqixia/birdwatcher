package set

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type IndexTypeParam struct {
	framework.ParamBase `use:"set index-type" desc:"set index type"`
	IndexID             int64  `name:"indexID" default:"0" desc:"index id to set"`
	Type                string `name:"type" default:"" desc:"index type to set"`
	Run                 bool   `name:"run" default:"false" desc:"flag to control actually run or dry"`
}

func (c *ComponentSet) IndexTypeCommand(ctx context.Context, p *IndexTypeParam) error {
	indexes, err := common.ListIndex(ctx, c.client, c.basePath, func(index *models.FieldIndex) bool {
		return index.GetProto().GetIndexInfo().GetIndexID() == p.IndexID
	})
	if err != nil {
		return err
	}

	if len(indexes) == 0 {
		return errors.Newf("no index found with index id %d", p.IndexID)
	}

	if !p.Run {
		fmt.Println("===Dry Run ===")
	}
	for _, index := range indexes {
		for _, kv := range index.GetProto().GetIndexInfo().GetIndexParams() {
			if kv.GetKey() == "index_type" {
				kv.Value = p.Type
			}
		}
		fmt.Println("index update:", index.GetProto().String())
		if p.Run {
			bs, err := proto.Marshal(index.GetProto())
			if err != nil {
				return err
			}
			err = c.client.Save(ctx, index.Key(), string(bs))
			if err != nil {
				return err
			}
			fmt.Println("Index Updated!")
		}
	}
	return nil
}
