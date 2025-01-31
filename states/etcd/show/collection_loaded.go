package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
)

const (
	ReplicaMetaPrefix = "queryCoord-ReplicaMeta"
)

type CollectionLoadedParam struct {
	framework.ParamBase `use:"show collection-loaded" desc:"display information of loaded collection from querycoord" alias:"collection-load"`
	CollectionID        int64 `name:"collection" default:"0" desc:"collection id to check"`
}

// CollectionLoadedCommand return show collection-loaded command.
func (c *ComponentShow) CollectionLoadedCommand(ctx context.Context, p *CollectionLoadedParam) (*CollectionsLoaded, error) {
	var total int
	infos, err := common.ListCollectionLoadedInfo(ctx, c.client, c.metaPath, etcdversion.GetVersion(), func(info *models.CollectionLoaded) bool {
		total++
		return p.CollectionID == 0 || p.CollectionID == info.CollectionID
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list collection load info")
	}

	return framework.NewListResult[CollectionsLoaded](infos), nil
}

type CollectionsLoaded struct {
	framework.ListResultSet[*models.CollectionLoaded]
}

func (rs *CollectionsLoaded) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, info := range rs.Data {
			rs.printCollectionLoaded(sb, info)
		}
		fmt.Fprintf(sb, "--- Collections Loaded: %d\n", len(rs.Data))
		return sb.String()
	default:
	}
	return ""
}

func (rs *CollectionsLoaded) printCollectionLoaded(sb *strings.Builder, info *models.CollectionLoaded) {
	fmt.Fprintf(sb, "Version: [%s]\tCollectionID: %d\n", info.Version, info.CollectionID)
	fmt.Fprintf(sb, "ReplicaNumber: %d", info.ReplicaNumber)
	switch info.Version {
	case models.LTEVersion2_1:
		fmt.Fprintf(sb, "\tInMemoryPercent: %d\n", info.InMemoryPercentage)
	case models.GTEVersion2_2:
		fmt.Fprintf(sb, "\tLoadStatus: %s\n", info.Status.String())
	}
	fmt.Fprintf(sb, "LoadFields: %v\n", info.LoadFields)
}
