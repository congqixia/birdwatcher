package set

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type EnableMatchCommand struct {
	framework.ParamBase `use:"set enable-match" desc:"set collection enable match"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to update"`
	FieldID             int64 `name:"field" default:"0" desc:"field id to update"`
	EnableMatch         bool  `name:"enableMatch" default:"false"`
	Run                 bool  `name:"run" default:"false"`
}

func (c *ComponentSet) EnableMatchCommand(ctx context.Context, p *EnableMatchCommand) error {
	collection, err := common.GetCollectionByIDVersion(ctx, c.client, c.basePath, p.CollectionID)
	if err != nil {
		return err
	}
	if collection == nil {
		return errors.Newf("collection with id %d not found", p.CollectionID)
	}

	return common.UpdateField(ctx, c.client, c.basePath, p.CollectionID, p.FieldID, func(field *schemapb.FieldSchema) {
		var foundKV bool
		for _, kv := range field.GetTypeParams() {
			if kv.Key == "enable_match" {
				foundKV = true
				kv.Value = fmt.Sprintf("%v", p.EnableMatch)
			}
		}
		if !foundKV && p.EnableMatch {
			field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
				Key:   "enable_match",
				Value: fmt.Sprintf("%v", p.EnableMatch),
			})
		}
	}, !p.Run)
}
