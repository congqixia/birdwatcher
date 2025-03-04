package show

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

type SegmentParam struct {
	framework.ParamBase `use:"show segment" desc:"display segment information from data coord meta store" alias:"segments"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	PartitionID         int64  `name:"partition" default:"0" desc:"partition id to filter with"`
	SegmentID           int64  `name:"segment" default:"0" desc:"segment id to display"`
	Format              string `name:"format" default:"line" desc:"segment display format"`
	LogType             string `name:"logType" default:"" desc:"specify the log type to display only in detail mode"`
	Detail              bool   `name:"detail" default:"false" desc:"flags indicating whether printing detail binlog info"`
	State               string `name:"state" default:"" desc:"target segment state"`
	Level               string `name:"level" default:"" desc:"target segment level"`
}

type segStats struct {
	// field id => log size
	binlogLogSize map[int64]int64
	// field id => mem size
	binlogMemSize map[int64]int64
	deltaLogSize  int64
	deltaMemSize  int64
	deltaEntryNum int64
	statsLogSize  int64
	statsMemSize  int64
}

// SegmentCommand returns show segments command.
func (c *ComponentShow) SegmentCommand(ctx context.Context, p *SegmentParam) error {
	segments, err := common.ListSegments(ctx, c.client, c.metaPath, func(segment *models.Segment) bool {
		return (p.CollectionID == 0 || segment.CollectionID == p.CollectionID) &&
			(p.PartitionID == 0 || segment.PartitionID == p.PartitionID) &&
			(p.SegmentID == 0 || segment.ID == p.SegmentID) &&
			(p.State == "" || strings.EqualFold(segment.State.String(), p.State)) &&
			(p.Level == "" || strings.EqualFold(segment.Level.String(), p.Level))
	})
	if err != nil {
		fmt.Println("failed to list segments", err.Error())
		return nil
	}

	totalRC := int64(0)
	healthy := 0

	var growing, sealed, flushed, dropped int
	var small, other int
	var smallCnt, otherCnt int64

	collectionID2SegStats := make(map[int64]*segStats)
	collectionID2Segments := lo.GroupBy(segments, func(s *models.Segment) int64 {
		return s.CollectionID
	})

	for collectionID, segs := range collectionID2Segments {
		fmt.Printf("===============================CollectionID: %d===========================\n", collectionID)
		collectionID2SegStats[collectionID] = &segStats{
			binlogLogSize: make(map[int64]int64),
			binlogMemSize: make(map[int64]int64),
		}

		for _, info := range segs {
			// if info.State != models.SegmentStateDropped {
			if info.State != commonpb.SegmentState_Dropped {
				totalRC += info.NumOfRows
				healthy++
			}
			switch info.State {
			case commonpb.SegmentState_Growing:
				growing++
			case commonpb.SegmentState_Sealed:
				sealed++
			case commonpb.SegmentState_Flushing, commonpb.SegmentState_Flushed:
				flushed++
				if float64(info.NumOfRows)/float64(info.MaxRowNum) < 0.2 {
					small++
					smallCnt += info.NumOfRows
				} else {
					other++
					otherCnt += info.NumOfRows
				}
			case commonpb.SegmentState_Dropped:
				dropped++
			}

			switch p.Format {
			case "table":
				PrintSegmentInfo(info, p.Detail, p.LogType)
			case "line":
				fmt.Printf("SegmentID: %d PartitionID: %d State: %s, Level: %s, Row Count:%d,  PartitionStatsVersion:%d, IsSorted: %v \n",
					info.ID, info.PartitionID, info.State.String(), info.Level.String(), info.NumOfRows, info.PartitionStatsVersion, info.IsSorted)
			case "statistics":
				if info.State != commonpb.SegmentState_Dropped {
					for _, binlog := range info.GetBinlogs() {
						for _, log := range binlog.Binlogs {
							collectionID2SegStats[collectionID].binlogLogSize[binlog.FieldID] += log.LogSize
							collectionID2SegStats[collectionID].binlogMemSize[binlog.FieldID] += log.MemSize
						}
					}
					for _, delta := range info.GetDeltalogs() {
						for _, log := range delta.Binlogs {
							collectionID2SegStats[collectionID].deltaLogSize += log.LogSize
							collectionID2SegStats[collectionID].deltaMemSize += log.MemSize
							collectionID2SegStats[collectionID].deltaEntryNum += log.EntriesNum
						}
					}
					for _, statslog := range info.GetStatslogs() {
						for _, binlog := range statslog.Binlogs {
							collectionID2SegStats[collectionID].statsLogSize += binlog.LogSize
							collectionID2SegStats[collectionID].statsMemSize += binlog.MemSize
						}
					}
				}
			default:
				err := fmt.Errorf("unsupport format:%s", p.Format)
				return err
			}
		}
		if p.Format == "statistics" {
			outputStats("Collection", collectionID2SegStats[collectionID])
		}
		fmt.Printf("\n")
	}

	if p.Format == "statistics" {
		outputStats("Total", lo.Values(collectionID2SegStats)...)
	}

	fmt.Printf("--- Growing: %d, Sealed: %d, Flushed: %d, Dropped: %d\n", growing, sealed, flushed, dropped)
	fmt.Printf("--- Small Segments: %d, row count: %d\t Other Segments: %d, row count: %d\n", small, smallCnt, other, otherCnt)
	fmt.Printf("--- Total Segments: %d, row count: %d\n", healthy, totalRC)
	return nil
}

func outputStats(scope string, stats ...*segStats) {
	var totalBinlogLogSize int64
	var totalBinlogMemSize int64
	var totalDeltaLogSize int64
	var totalDeltaMemSize int64
	var totalDeltaEntryNum int64
	var totalStatsLogSize int64
	var totalStatsMemSize int64
	for _, s := range stats {
		for fieldID, logSize := range s.binlogLogSize {
			memSize := s.binlogMemSize[fieldID]
			if scope != "Total" {
				fmt.Printf("field[%d] binlog size: %s, mem size: %s\n", fieldID, hrSize(logSize), hrSize(memSize))
			}
			totalBinlogLogSize += logSize
			totalBinlogMemSize += memSize
		}

		totalDeltaLogSize += s.deltaLogSize
		totalDeltaMemSize += s.deltaMemSize
		totalDeltaEntryNum += s.deltaEntryNum
		totalStatsLogSize += s.statsLogSize
		totalStatsMemSize += s.statsMemSize
	}

	fmt.Printf("--- %s binlog size: %s, mem size: %s\n", scope, hrSize(totalBinlogLogSize), hrSize(totalBinlogMemSize))
	fmt.Printf("--- %s deltalog size: %s, mem size: %s, delta entry number: %d\n", scope, hrSize(totalDeltaLogSize), hrSize(totalDeltaMemSize), totalDeltaEntryNum)
	fmt.Printf("--- %s statslog size: %s, mem size: %s\n", scope, hrSize(totalStatsLogSize), hrSize(totalStatsMemSize))
}

func hrSize(size int64) string {
	sf := float64(size)
	units := []string{"Bytes", "KB", "MB", "GB"}
	idx := 0
	for sf > 1024.0 && idx < 3 {
		sf /= 1024.0
		idx++
	}
	return fmt.Sprintf("%f %s", sf, units[idx])
}

const (
	tsPrintFormat = "2006-01-02 15:04:05.999 -0700"
)

// PrintSegmentInfo prints segments info
func PrintSegmentInfo(info *models.Segment, detailBinlog bool, logType string) {
	fmt.Println("================================================================================")
	fmt.Printf("Segment ID: %d\n", info.ID)
	fmt.Printf("Segment State: %v", info.State)
	if info.State == commonpb.SegmentState_Dropped {
		dropTime := time.Unix(0, int64(info.DroppedAt))
		fmt.Printf("\tDropped Time: %s", dropTime.Format(tsPrintFormat))
	}
	fmt.Printf("\tSegment Level: %s", info.Level.String())
	fmt.Println()
	fmt.Printf("Collection ID: %d\t\tPartitionID: %d\n", info.CollectionID, info.PartitionID)
	fmt.Printf("Insert Channel:%s\n", info.InsertChannel)
	fmt.Printf("Num of Rows: %d\t\tMax Row Num: %d\n", info.NumOfRows, info.MaxRowNum)
	lastExpireTime, _ := utils.ParseTS(info.LastExpireTime)
	fmt.Printf("Last Expire Time: %s\n", lastExpireTime.Format(tsPrintFormat))
	fmt.Printf("Compact from %v \n", info.CompactionFrom)
	if info.StartPosition != nil {
		startTime, _ := utils.ParseTS(info.GetStartPosition().GetTimestamp())
		fmt.Printf("Start Position ID: %v, time: %s, channel name %s\n", info.GetStartPosition().MsgID, startTime.Format(tsPrintFormat), info.GetStartPosition().GetChannelName())
	} else {
		fmt.Println("Start Position: nil")
	}
	if info.DmlPosition != nil {
		dmlTime, _ := utils.ParseTS(info.DmlPosition.Timestamp)
		fmt.Printf("Dml Position ID: %v, time: %s, channel name: %s\n", info.DmlPosition.MsgID, dmlTime.Format(tsPrintFormat), info.GetDmlPosition().GetChannelName())
	} else {
		fmt.Println("Dml Position: nil")
	}
	fmt.Printf("Binlog Nums %d\tStatsLog Nums: %d\tDeltaLog Nums:%d\n",
		countBinlogNum(info.GetBinlogs()), countBinlogNum(info.GetStatslogs()), countBinlogNum(info.GetDeltalogs()))

	if detailBinlog {
		if logType == "" || strings.EqualFold(logType, "binlog") {
			var binlogSize int64
			var insertmemSize int64
			fmt.Println("**************************************")
			fmt.Println("Binlogs:")
			sort.Slice(info.GetBinlogs(), func(i, j int) bool {
				return info.GetBinlogs()[i].FieldID < info.GetBinlogs()[j].FieldID
			})
			for _, log := range info.GetBinlogs() {
				var fieldLogSize int64
				fmt.Printf("Field %d:\n", log.FieldID)
				for _, binlog := range log.Binlogs {
					fmt.Printf("Path: %s\n", binlog.LogPath)
					tf, _ := utils.ParseTS(binlog.TimestampFrom)
					tt, _ := utils.ParseTS(binlog.TimestampTo)
					fmt.Printf("LogID: %d \t Mem Size: %d \t Log Size: %d \t Entry Num: %d\t TimeRange:%s-%s\n",
						binlog.LogID, binlog.MemSize,
						binlog.LogSize, binlog.EntriesNum,
						tf.Format(tsPrintFormat), tt.Format(tsPrintFormat))
					binlogSize += binlog.LogSize
					insertmemSize += binlog.MemSize
					fieldLogSize += binlog.LogSize
				}
				fmt.Println("--- Field Log Size:", hrSize(fieldLogSize))
			}
			fmt.Println("=== Segment Total Binlog Size: ", hrSize(binlogSize))
			fmt.Println("=== Segment Total Binlog Mem Size: ", hrSize(insertmemSize))
		}

		if logType == "" || strings.EqualFold(logType, "statslog") {
			fmt.Println("**************************************")
			fmt.Println("Statslogs:")
			sort.Slice(info.GetStatslogs(), func(i, j int) bool {
				return info.GetStatslogs()[i].FieldID < info.GetStatslogs()[j].FieldID
			})
			var statsLogSize int64
			for _, log := range info.GetStatslogs() {
				fmt.Printf("Field %d:\n", log.FieldID)
				for _, binlog := range log.Binlogs {
					fmt.Printf("Path: %s\n", binlog.LogPath)
					tf, _ := utils.ParseTS(binlog.TimestampFrom)
					tt, _ := utils.ParseTS(binlog.TimestampTo)
					fmt.Printf("LogID: %d \t Log Size: %d \t Entry Num: %d\t TimeRange:%s-%s\n",
						binlog.LogID, binlog.LogSize, binlog.EntriesNum,
						tf.Format(tsPrintFormat), tt.Format(tsPrintFormat))
					statsLogSize += binlog.LogSize
				}
			}
			fmt.Println("=== Segment Total Statslog Size: ", hrSize(statsLogSize))
		}

		if logType == "" || strings.EqualFold(logType, "deltalog") {
			fmt.Println("**************************************")
			fmt.Println("Delta Logs:")
			var deltaLogSize int64
			var memSize int64
			var deltaEntryNum int64
			for _, log := range info.GetDeltalogs() {
				for _, l := range log.Binlogs {
					fmt.Printf("Entries: %d From: %v - To: %v\n", l.EntriesNum, l.TimestampFrom, l.TimestampTo)
					fmt.Printf("LogID: %d, Path: %v LogSize: %s, MemSize: %s\n", l.LogID, l.LogPath, hrSize(l.LogSize), hrSize(l.MemSize))
					deltaLogSize += l.LogSize
					memSize += l.MemSize
					deltaEntryNum += l.EntriesNum
				}
			}
			fmt.Println("=== Segment Total Delta entry num: ", deltaEntryNum)
			fmt.Println("=== Segment Total Deltalog Size: ", hrSize(deltaLogSize))
			fmt.Println("=== Segment Total Deltalog Mem Size: ", hrSize(memSize))
		}
	}

	fmt.Println("================================================================================")
}

func countBinlogNum(fbl []*models.FieldBinlog) int {
	result := 0
	for _, f := range fbl {
		result += len(f.Binlogs)
	}
	return result
}
