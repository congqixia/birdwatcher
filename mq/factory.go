//go:build !WKAFKA
// +build !WKAFKA

package mq

import (
	"fmt"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/birdwatcher/mq/ifc"
	"github.com/milvus-io/birdwatcher/mq/pulsar"
)

func ParsePositionFromCheckpoint(mqType string, messageID []byte) (ifc.MessageID, error) {
	switch mqType {
	case "pulsar":
		return pulsar.DeserializePulsarMsgID(messageID)
	default:
		return nil, errors.Newf("not supported mq type: %s", mqType)
	}
}

func ParseManualMessageID(mqType string, manualID string) (ifc.MessageID, error) {
	// pulsar not supported yet
	switch mqType {
	case "pulsar":
		return pulsar.StringToMsgID(manualID)
	default:
		return nil, errors.Newf("not supported mq type: %s", mqType)
	}
}

func NewConsumer(mqType, address, channel string, config ifc.MqOption) (ifc.Consumer, error) {
	groupID := fmt.Sprintf("group-id-%d", time.Now().UnixNano())
	switch mqType {
	case "pulsar":
		return pulsar.NewPulsarConsumer(address, channel, groupID, config)
	default:
		panic("unknown mq type:" + mqType)
	}
}
