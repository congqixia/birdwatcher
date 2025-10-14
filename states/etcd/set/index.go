package set

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type SetIndexParamsParam struct {
	framework.ParamBase `use:"set index-param"`
	IndexID             int64  `name:"indexID" default:"0"`
	Key                 string `name:"key" default:""`
	Value               string `name:"value" default:""`
	Run                 bool   `name:"run" default:"false"`
}

func (c *ComponentSet) SetIndexParamsCommand(ctx context.Context, p *SetIndexParamsParam) error {
	if p.IndexID == 0 {
		return errors.New("index id not provided")
	}
	fieldIndexes, err := common.ListIndex(ctx, c.client, c.basePath, func(info *models.FieldIndex) bool {
		return info.GetProto().GetIndexInfo().GetIndexID() == p.IndexID
	})

	if err != nil {
		return err
	}

	if len(fieldIndexes) == 0 {
		return errors.New("index not found")
	}

	for _, index := range fieldIndexes {
		fmt.Printf("Hit index: %s\n", index.GetProto().String())
		indexParams := index.GetProto().GetIndexInfo().GetIndexParams()

		found := false
		for _, param := range indexParams {
			if param.GetKey() == p.Key {
				param.Value = p.Value
				found = true
				break
			}
		}
		if !found {
			indexParams = append(indexParams, &commonpb.KeyValuePair{
				Key:   p.Key,
				Value: p.Value,
			})
		}
		index.GetProto().IndexInfo.IndexParams = indexParams
		fmt.Printf("After update: %s\n", index.GetProto().String())
	}
	if !p.Run {
		fmt.Println("===Dry Run ===")
		return nil
	}

	for _, index := range fieldIndexes {
		bs, err := proto.Marshal(index.GetProto())
		if err != nil {
			return err
		}
		err = c.client.Save(ctx, index.Key(), string(bs))
		if err != nil {
			return err
		}
	}

	return nil
}
