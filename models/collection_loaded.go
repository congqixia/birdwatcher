package models

import (
	"sort"

	"github.com/milvus-io/birdwatcher/proto/v2.0/querypb"
	querypbv2 "github.com/milvus-io/birdwatcher/proto/v2.2/querypb"
)

// CollectionLoaded models collection loaded information.
type CollectionLoaded struct {
	CollectionID int64
	/* v1 deprecated
	PartitionIDs []int64
	PartitionStates []PartitionState
	*/
	LoadType             LoadType
	Schema               CollectionSchema
	ReleasedPartitionIDs []int64
	InMemoryPercentage   int64
	ReplicaIDs           []int64
	ReplicaNumber        int32
	// since 2.2
	Status       LoadStatus
	FieldIndexID map[int64]int64
	LoadFields   []int64

	// orignial etcd Key
	Key     string
	Version string
}

func newCollectionLoadedBase[base interface {
	GetCollectionID() int64
	GetReplicaNumber() int32
}](info base) *CollectionLoaded {
	return &CollectionLoaded{
		CollectionID:  info.GetCollectionID(),
		ReplicaNumber: info.GetReplicaNumber(),
	}
}

func NewCollectionLoadedV2_1(info *querypb.CollectionInfo, key string) *CollectionLoaded {
	c := newCollectionLoadedBase(info)
	c.ReleasedPartitionIDs = info.GetPartitionIDs()

	c.LoadType = LoadType(info.GetLoadType())
	c.Schema = newSchemaFromBase(info.GetSchema())
	c.InMemoryPercentage = info.GetInMemoryPercentage()
	c.ReplicaIDs = info.GetReplicaIds()
	c.Version = LTEVersion2_1
	c.Key = key

	return c
}

func NewCollectionLoadedV2_2(info *querypbv2.CollectionLoadInfo, key string) *CollectionLoaded {
	c := newCollectionLoadedBase(info)
	c.ReleasedPartitionIDs = info.GetReleasedPartitions()
	c.Status = LoadStatus(info.GetStatus())
	c.FieldIndexID = info.GetFieldIndexID()
	c.LoadFields = info.GetLoadFields()
	sort.Slice(c.LoadFields, func(i, j int) bool {
		return c.LoadFields[i] < c.LoadFields[j]
	})
	c.Version = GTEVersion2_2
	c.Key = key
	return c
}

type LoadType int32

const (
	LoadTypeUnKnownType    LoadType = 0
	LoadTypeLoadPartition  LoadType = 1
	LoadTypeLoadCollection LoadType = 2
)

var LoadTypename = map[int32]string{
	0: "UnKnownType",
	1: "LoadPartition",
	2: "LoadCollection",
}

var LoadTypevalue = map[string]int32{
	"UnKnownType":    0,
	"LoadPartition":  1,
	"LoadCollection": 2,
}

func (x LoadType) String() string {
	return EnumName(LoadTypename, int32(x))
}

type LoadStatus int32

const (
	LoadStatusInvalid LoadStatus = 0
	LoadStatusLoading LoadStatus = 1
	LoadStatusLoaded  LoadStatus = 2
)

var LoadStatusname = map[int32]string{
	0: "Invalid",
	1: "Loading",
	2: "Loaded",
}

var LoadStatusvalue = map[string]int32{
	"Invalid": 0,
	"Loading": 1,
	"Loaded":  2,
}

func (x LoadStatus) String() string {
	return EnumName(LoadStatusname, int32(x))
}
