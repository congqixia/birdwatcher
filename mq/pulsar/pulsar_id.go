package pulsar

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"

	"github.com/milvus-io/birdwatcher/mq/ifc"
)

type pulsarID struct {
	messageID pulsar.MessageID
}

func (pid *pulsarID) String() string {
	return fmt.Sprintf("messageID: paritionID(%d), ledgerID(%d),"+
		" entryID(%d), BatchID(%d)", pid.messageID.PartitionIdx(), pid.messageID.LedgerID(),
		pid.messageID.EntryID(), pid.messageID.BatchIdx())
}

// Check if pulsarID implements and MessageID ifc
var _ ifc.MessageID = &pulsarID{}

func (pid *pulsarID) Serialize() []byte {
	return pid.messageID.Serialize()
}

func (pid *pulsarID) AtEarliestPosition() bool {
	if pid.messageID.PartitionIdx() <= 0 &&
		pid.messageID.LedgerID() <= 0 &&
		pid.messageID.EntryID() <= 0 &&
		pid.messageID.BatchIdx() <= 0 {
		return true
	}
	return false
}

func (pid *pulsarID) LessOrEqualThan(msgID []byte) (bool, error) {
	pMsgID, err := pulsar.DeserializeMessageID(msgID)
	if err != nil {
		return false, err
	}

	if pid.messageID.LedgerID() <= pMsgID.LedgerID() &&
		pid.messageID.EntryID() <= pMsgID.EntryID() &&
		pid.messageID.BatchIdx() <= pMsgID.BatchIdx() {
		return true, nil
	}

	return false, nil
}

func (pid *pulsarID) Equal(msgID []byte) (bool, error) {
	pMsgID, err := pulsar.DeserializeMessageID(msgID)
	if err != nil {
		return false, err
	}

	if pid.messageID.LedgerID() == pMsgID.LedgerID() &&
		pid.messageID.EntryID() == pMsgID.EntryID() &&
		pid.messageID.BatchIdx() == pMsgID.BatchIdx() {
		return true, nil
	}

	return false, nil
}

// SerializePulsarMsgID returns the serialized message ID
func SerializePulsarMsgID(messageID pulsar.MessageID) []byte {
	return messageID.Serialize()
}

// DeserializePulsarMsgID returns the deserialized message ID
func DeserializePulsarMsgID(messageID []byte) (ifc.MessageID, error) {
	id, err := pulsar.DeserializeMessageID(messageID)
	if err != nil {
		return nil, err
	}
	return &pulsarID{messageID: id}, nil
}

// msgIDToString is used to convert a message ID to string
func msgIDToString(messageID pulsar.MessageID) string {
	return strings.ToValidUTF8(string(messageID.Serialize()), "")
}

// StringToMsgID is used to convert a string to message ID
func StringToMsgID(msgString string) (ifc.MessageID, error) {
	ids := strings.Split(msgString, ":")
	if len(ids) != 2 {
		return nil, errors.Newf("invalid manual id: %s", msgString)
	}
	ledgerID, err := strconv.ParseUint(ids[0], 10, 64)
	if err != nil {
		return nil, err
	}
	entryID, err := strconv.ParseUint(ids[1], 10, 64)
	if err != nil {
		return nil, err
	}
	msgID := newMessageIDOfPulsar(ledgerID, entryID, 0)
	return DeserializePulsarMsgID(msgID.Serialize())
}

// newMessageIDOfPulsar only for test.
func newMessageIDOfPulsar(ledgerID uint64, entryID uint64, batchIdx int32) pulsar.MessageID {
	partitionIdx := int32(0)
	id := &MessageIdData{
		LedgerId:   &ledgerID,
		EntryId:    &entryID,
		BatchIndex: &batchIdx,
		Partition:  &partitionIdx,
	}

	msg, err := proto.Marshal(id)
	if err != nil {
		panic(err)
	}
	msgID, err := pulsar.DeserializeMessageID(msg)
	if err != nil {
		panic(err)
	}
	return msgID
}
