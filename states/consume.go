package states

import (
	"context"
	"fmt"
	"path"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/mq"
	"github.com/milvus-io/birdwatcher/mq/ifc"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type ConsumeParam struct {
	framework.ParamBase `use:"consume" desc:"consume msgs from provided topic"`
	StartPosition       string `name:"start_pos" default:"cp" desc:"position to start with"`
	MqType              string `name:"mq_type" default:"pulsar" desc:"message queue type to consume"`
	MqAddress           string `name:"mq_addr" default:"pulsar://127.0.0.1:6650" desc:"message queue service address"`
	Topic               string `name:"topic" default:"" desc:"topic to consume"`
	ShardName           string `name:"shard_name" default:"" desc:"shard name(vchannel name) to filter with"`
	Detail              bool   `name:"detail" default:"false" desc:"print msg detail"`
	ManualID            int64  `name:"manual_id" default:"0" desc:"manual id"`
}

func (s *InstanceState) ConsumeCommand(ctx context.Context, p *ConsumeParam) error {
	var messageID ifc.MessageID
	switch p.StartPosition {
	case "cp":
		prefix := path.Join(s.basePath, "datacoord-meta", "channel-cp", p.ShardName)
		results, _, err := common.ListProtoObjects[msgpb.MsgPosition](ctx, s.client, prefix)
		if err != nil {
			return err
		}
		if len(results) == 1 {
			checkpoint := results[0]
			messageID, err = mq.ParsePositionFromCheckpoint(p.MqType, checkpoint.GetMsgID())
			if err != nil {
				return err
			}
		}
	case "manual":
		var err error
		messageID, err = mq.ParseManualMessageID(p.MqType, p.ManualID)
		if err != nil {
			return err
		}
	default:
	}

	subPos := ifc.SubscriptionPositionEarliest
	if messageID != nil {
		subPos = ifc.SubscriptionPositionLatest
	}

	c, err := mq.NewConsumer(p.MqType, p.MqAddress, p.Topic, ifc.MqOption{
		SubscriptionInitPos: subPos,
	})
	if err != nil {
		return err
	}

	if messageID != nil {
		fmt.Println("Using message ID to seek", messageID)
		err := c.Seek(messageID)
		if err != nil {
			return err
		}
	}

	latestID, err := c.GetLastMessageID()
	if err != nil {
		return err
	}
	if latestID.AtEarliestPosition() {
		fmt.Println("empty topic")
		return nil
	}

	for {
		if ctx.Err() != nil {
			break
		}
		msg, err := c.Consume()
		if err != nil {
			return err
		}
		header := commonpb.MsgHeader{}
		proto.Unmarshal(msg.Payload(), &header)
		msgType := header.GetBase().GetMsgType()
		if msgType != commonpb.MsgType_TimeTick {
			// fmt.Printf("%s ", msgType)
			switch msgType {
			case commonpb.MsgType_Insert, commonpb.MsgType_Delete:
				v, err := ParseMsg(header.GetBase().GetMsgType(), msg.Payload())
				if err != nil {
					fmt.Println(err.Error())
				}
				if v != nil && (p.ShardName == "" || v.GetShardName() == p.ShardName) {
					if p.Detail {
						fmt.Print(v)
					} else {
						fmt.Print(v.GetShardName())
						err := ValidateMsg(msgType, msg.Payload())
						if err != nil {
							fmt.Println(err.Error())
						}
					}
				}
			default:
			}
		}
		if eq, _ := msg.ID().Equal(latestID.Serialize()); eq {
			break
		}
	}
	return nil
}

func ParseInsertMsg(msgType commonpb.MsgType, payload []byte) (*msgpb.InsertRequest, error) {
	if msgType != commonpb.MsgType_Insert {
		return nil, errors.Newf("expected message type %s, got %s", commonpb.MsgType_Insert, msgType)
	}
	msg := &msgpb.InsertRequest{}
	err := proto.Unmarshal(payload, msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func ParseMsg(msgType commonpb.MsgType, payload []byte) (interface {
	proto.Message
	GetShardName() string
}, error,
) {
	var msg interface {
		proto.Message
		GetShardName() string
	}
	switch msgType {
	case commonpb.MsgType_Insert:
		msg = &msgpb.InsertRequest{}
	case commonpb.MsgType_Delete:
		msg = &msgpb.DeleteRequest{}
	}
	err := proto.Unmarshal(payload, msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func ValidateMsg(msgType commonpb.MsgType, payload []byte) error {
	switch msgType {
	case commonpb.MsgType_Insert:
		msg := &msgpb.InsertRequest{}
		proto.Unmarshal(payload, msg)
		for _, fieldData := range msg.GetFieldsData() {
			msgType := fieldData.GetType()
			switch msgType {
			case schemapb.DataType_Int64:
				l := len(fieldData.GetScalars().GetLongData().GetData())
				if l != int(msg.GetNumRows()) {
					return errors.Newf("Field %d(%s) len = %d, datatype %v mismatch num rows: %d", fieldData.GetFieldId(), fieldData.GetFieldName(), l, msgType, msg.GetNumRows())
				}
			case schemapb.DataType_VarChar:
				l := len(fieldData.GetScalars().GetStringData().GetData())
				if l != int(msg.GetNumRows()) {
					return errors.Newf("Field %d(%s) len = %d, datatype %v mismatch num rows: %d", fieldData.GetFieldId(), fieldData.GetFieldName(), l, msgType, msg.GetNumRows())
				}
			case schemapb.DataType_Bool:
				l := len(fieldData.GetScalars().GetBoolData().GetData())
				if l != int(msg.GetNumRows()) {
					return errors.Newf("Field %d(%s) len = %d, datatype %v mismatch num rows: %d", fieldData.GetFieldId(), fieldData.GetFieldName(), l, msgType, msg.GetNumRows())
				}
			case schemapb.DataType_FloatVector:
				l := len(fieldData.GetVectors().GetFloatVector().GetData())
				dim := fieldData.GetVectors().GetDim()
				if l/int(dim) != int(msg.GetNumRows()) {
					return errors.Newf("Field %d(%s) len = %d, datatype %v mismatch num rows: %d", fieldData.GetFieldId(), fieldData.GetFieldName(), l, msgType, msg.GetNumRows())
				}
			default:
				fmt.Println("skip unhandled data type", fieldData.GetType())
			}
		}
	case commonpb.MsgType_Delete:
		// TODO maybe process delete as well?
	default:
		fmt.Printf("not supported message type: %s", msgType.String())
	}
	return nil
}
