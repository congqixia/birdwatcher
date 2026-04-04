package show

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
)

// CollectionCommand returns sub command for showCmd.
// show collection [options...]
type CollectionParam struct {
	framework.DataSetParam `use:"show collections" desc:"list current available collection from RootCoord"`
	CollectionID           int64  `name:"id" default:"0" desc:"collection id to display"`
	CollectionName         string `name:"name" default:"" desc:"collection name to display"`
	DatabaseID             int64  `name:"dbid" default:"-1" desc:"database id to filter"`
	State                  string `name:"state" default:"" desc:"collection state to filter"`
	WithPropertyKey        string `name:"propertyKey" default:"" desc:"collection property to filter"`
	HasStructArray         bool   `name:"hasStructArray" default:"false" desc:"filter collections with struct array fields"`
}

func (c *ComponentShow) CollectionCommand(ctx context.Context, p *CollectionParam) (*framework.PresetResultSet, error) {
	var collections []*models.Collection
	var total int64
	var err error
	// perform get by id to accelerate
	if p.CollectionID > 0 {
		var collection *models.Collection
		collection, err = common.GetCollectionByIDVersion(ctx, c.client, c.metaPath, p.CollectionID)
		if err == nil {
			collections = append(collections, collection)
		}
	} else {
		collections, err = common.ListCollections(ctx, c.client, c.metaPath, func(info *models.Collection) bool {
			coll := info.GetProto()
			if p.CollectionName != "" && coll.Schema.Name != p.CollectionName {
				return false
			}
			if p.DatabaseID > -1 && coll.DbId != p.DatabaseID {
				return false
			}
			if p.State != "" && !strings.EqualFold(p.State, coll.State.String()) {
				return false
			}

			if p.WithPropertyKey != "" {
				found := false
				for _, prop := range coll.Properties {
					if prop.Key == p.WithPropertyKey {
						found = true
						break
					}
				}
				if !found {
					return false
				}
			}

			if p.HasStructArray && len(coll.GetSchema().GetStructArrayFields()) == 0 {
				return false
			}

			total++
			return true
		})
	}

	collections = lo.Filter(collections, func(c *models.Collection, _ int) bool {
		return !p.HasStructArray || len(c.GetProto().GetSchema().GetStructArrayFields()) > 0
	})

	if err != nil {
		return nil, err
	}
	channels := 0
	healthy := 0
	for _, collection := range collections {
		if collection.GetProto().State == etcdpb.CollectionState_CollectionCreated {
			channels += len(collection.GetProto().GetVirtualChannelNames())
			healthy++
		}
	}

	return framework.NewPresetResultSet(&Collections{
		collections: collections,
		total:       total,
		channels:    channels,
		healthy:     healthy,
	}, framework.NameFormat(p.Format)), nil
}

type Collections struct {
	collections []*models.Collection
	total       int64
	channels    int
	healthy     int
}

func (rs *Collections) TableHeaders() table.Row {
	return table.Row{"ID", "Name", "DBID", "State", "Fields", "Channels"}
}

func (rs *Collections) TableRows() []table.Row {
	rows := make([]table.Row, 0, len(rs.collections))
	for _, coll := range rs.collections {
		proto := coll.GetProto()
		rows = append(rows, table.Row{
			proto.ID, proto.Schema.Name, proto.DbId,
			proto.State.String(), len(proto.Schema.Fields),
			len(coll.Channels()),
		})
	}
	return rows
}

func (rs *Collections) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, coll := range rs.collections {
			printCollection(sb, coll)
		}
		fmt.Fprintln(sb, "================================================================================")
		fmt.Fprintf(sb, "--- Total collections:  %d\t Matched collections:  %d\n", rs.total, len(rs.collections))
		fmt.Fprintf(sb, "--- Total channel: %d\t Healthy collections: %d\n", rs.channels, rs.healthy)
		return sb.String()
	case framework.FormatJSON:
		return rs.printAsJSON()
	case framework.FormatLine:
		sb := &strings.Builder{}
		for _, coll := range rs.collections {
			fmt.Fprintf(sb, "collection id %d\t collection name %s\n", coll.GetProto().ID, coll.GetProto().Schema.Name)
		}
		return sb.String()
	}
	return ""
}

func (rs *Collections) printAsJSON() string {
	type FieldJSON struct {
		FieldID        int64             `json:"field_id"`
		Name           string            `json:"name"`
		DataType       string            `json:"data_type"`
		IsPrimaryKey   bool              `json:"is_primary_key,omitempty"`
		AutoID         bool              `json:"auto_id,omitempty"`
		IsPartitionKey bool              `json:"is_partition_key,omitempty"`
		IsDynamic      bool              `json:"is_dynamic,omitempty"`
		Nullable       bool              `json:"nullable,omitempty"`
		TypeParams     map[string]string `json:"type_params,omitempty"`
	}

	type ChannelJSON struct {
		PhysicalName string `json:"physical_name"`
		VirtualName  string `json:"virtual_name"`
	}

	type CollectionJSON struct {
		ID               int64             `json:"id"`
		Name             string            `json:"name"`
		DBID             int64             `json:"db_id"`
		State            string            `json:"state"`
		CreateTime       string            `json:"create_time"`
		UpdateTime       string            `json:"update_time"`
		SchemaVersion    int32             `json:"schema_version"`
		ConsistencyLevel string            `json:"consistency_level"`
		Fields           []FieldJSON       `json:"fields"`
		Channels         []ChannelJSON     `json:"channels"`
		Properties       map[string]string `json:"properties,omitempty"`
	}

	type OutputJSON struct {
		Collections      []CollectionJSON `json:"collections"`
		TotalCollections int64            `json:"total_collections"`
		MatchedCount     int              `json:"matched_count"`
		TotalChannels    int              `json:"total_channels"`
		HealthyCount     int              `json:"healthy_count"`
	}

	output := OutputJSON{
		Collections:      make([]CollectionJSON, 0, len(rs.collections)),
		TotalCollections: rs.total,
		MatchedCount:     len(rs.collections),
		TotalChannels:    rs.channels,
		HealthyCount:     rs.healthy,
	}

	for _, coll := range rs.collections {
		proto := coll.GetProto()
		createTime, _ := utils.ParseTS(proto.CreateTime)
		updateTime, _ := utils.ParseTS(proto.UpdateTimestamp)

		fields := make([]FieldJSON, 0, len(proto.Schema.Fields))
		for _, f := range proto.Schema.Fields {
			typeParams := make(map[string]string)
			for _, kv := range f.TypeParams {
				typeParams[kv.Key] = kv.Value
			}
			fields = append(fields, FieldJSON{
				FieldID:        f.FieldID,
				Name:           f.Name,
				DataType:       f.DataType.String(),
				IsPrimaryKey:   f.IsPrimaryKey,
				AutoID:         f.AutoID,
				IsPartitionKey: f.IsPartitionKey,
				IsDynamic:      f.IsDynamic,
				Nullable:       f.GetNullable(),
				TypeParams:     typeParams,
			})
		}

		channels := make([]ChannelJSON, 0, len(coll.Channels()))
		for _, ch := range coll.Channels() {
			channels = append(channels, ChannelJSON{
				PhysicalName: ch.PhysicalName,
				VirtualName:  ch.VirtualName,
			})
		}

		props := make(map[string]string)
		for _, kv := range proto.Properties {
			props[kv.GetKey()] = kv.GetValue()
		}

		output.Collections = append(output.Collections, CollectionJSON{
			ID:               proto.ID,
			Name:             proto.Schema.Name,
			DBID:             proto.DbId,
			State:            proto.State.String(),
			CreateTime:       createTime.Format("2006-01-02 15:04:05"),
			UpdateTime:       updateTime.Format("2006-01-02 15:04:05"),
			SchemaVersion:    proto.Schema.Version,
			ConsistencyLevel: proto.ConsistencyLevel.String(),
			Fields:           fields,
			Channels:         channels,
			Properties:       props,
		})
	}

	return framework.MarshalJSON(output)
}

func (rs *Collections) Entities() any {
	return rs.collections
}

func printCollection(sb *strings.Builder, info *models.Collection) {
	collection := info.GetProto()
	fmt.Fprintln(sb, "================================================================================")
	fmt.Fprintf(sb, "DBID: %d\n", collection.DbId)
	fmt.Fprintf(sb, "Collection ID: %d\tCollection Name: %s\n", collection.ID, collection.Schema.Name)
	createTime, _ := utils.ParseTS(collection.CreateTime)
	updateTime, _ := utils.ParseTS(collection.UpdateTimestamp)
	fmt.Fprintf(sb, "Collection State: %s\tCreate Time: %s\n", collection.State.String(), createTime.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(sb, "Update Time: %s\tUpdate timestamp: %d\n", updateTime.Format("2006-01-02 15:04:05"), collection.UpdateTimestamp)
	fmt.Fprintf(sb, "Schema Version: %d\n", collection.Schema.Version)
	fmt.Fprintf(sb, "Fields:\n")
	fields := collection.Schema.Fields
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].FieldID < fields[j].FieldID
	})
	for _, field := range fields {
		fmt.Fprintf(sb, " - Field ID: %d \t Field Name: %s \t Field Type: %s\n", field.FieldID, field.Name, field.DataType.String())
		if field.IsPrimaryKey {
			fmt.Fprintf(sb, "\t - Primary Key: %t, AutoID: %t\n", field.IsPrimaryKey, field.AutoID)
		}
		if field.GetNullable() {
			fmt.Fprintf(sb, "\t - %s\n", color.MagentaString("Nullable"))
		}
		if field.GetDefaultValue() != nil {
			fmt.Fprintf(sb, "\t - %s: %v\n", color.MagentaString("DefaultValue"), field.GetDefaultValue())
		}
		if field.IsDynamic {
			fmt.Fprintf(sb, "\t - Dynamic Field\n")
		}
		if field.IsPartitionKey {
			fmt.Fprintf(sb, "\t - Partition Key\n")
		}
		if field.IsClusteringKey {
			fmt.Fprintf(sb, "\t - Clustering Key\n")
		}
		if field.IsFunctionOutput {
			fmt.Fprintf(sb, "\t - Function Output\n")
		}
		// print element type if field is array
		if field.DataType == schemapb.DataType_Array {
			fmt.Fprintf(sb, "\t - Element Type:  %s\n", field.ElementType.String())
		}
		// type params
		for _, kv := range field.TypeParams {
			fmt.Fprintf(sb, "\t - Type Param %s: %s\n", kv.Key, kv.Value)
		}
	}
	for _, structField := range info.GetProto().GetSchema().GetStructArrayFields() {
		fmt.Fprintf(sb, " - Struct Field ID: %d \t Field Name: %s\n", structField.FieldID, structField.Name)
		for _, field := range structField.Fields {
			fmt.Fprintf(sb, "\t - Field ID: %d \t Field Name: %s \t Field Type: %s\n", field.FieldID, field.Name, field.DataType.String())
		}
	}

	fmt.Fprintf(sb, "Enable Dynamic Schema: %t\n", collection.Schema.EnableDynamicField)
	for _, function := range info.Functions {
		fp := function.GetProto()
		fmt.Fprintf(sb, "Function Name: %s, Type: %s, Input: %v, Output: %v\n", fp.GetName(), fp.GetType().String(), fp.GetInputFieldNames(), fp.GetOutputFieldNames())
	}
	fmt.Fprintf(sb, "Consistency Level: %s\n", collection.ConsistencyLevel.String())
	fmt.Fprintf(sb, "Shard Infos(%d):\n", len(info.Channels()))
	for idx, channel := range info.Channels() {
		lastTruncateTimeTick := uint64(0)
		shardInfos := info.GetProto().GetShardInfos()
		if idx < len(shardInfos) {
			lastTruncateTimeTick = shardInfos[idx].LastTruncateTimeTick
		}
		fmt.Fprintf(sb, " - Channel: %s(%s), StartPosition: %v, LastTruncateTimeTick: %d\n", channel.PhysicalName, channel.VirtualName, channel.StartPosition.MsgID, lastTruncateTimeTick)
	}
	fmt.Fprintf(sb, "Collection properties(%d):\n", len(collection.Properties))
	for _, kv := range collection.Properties {
		fmt.Fprintf(sb, "\tKey: %s: %v\n", kv.GetKey(), kv.GetValue())
	}
}
