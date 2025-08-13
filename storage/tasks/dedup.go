package tasks

import (
	"fmt"
	"os"
	"sync"

	"go.uber.org/atomic"

	"github.com/gosuri/uilive"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/storage/common"
)

type DedupTask struct {
	baseScanTask
	limit   int64
	pkField models.FieldSchema
	// dedup
	ids         sync.Map // pk set
	dedupResult sync.Map // id => duplicated count
	dumpFile    string
}

func (t *DedupTask) Scan(pk common.PrimaryKey, batchInfo *common.BatchInfo, offset int, values map[int64]any) error {
	pkv := pk.GetValue()
	_, ok := t.ids.LoadOrStore(pkv, struct{}{})
	if ok {
		t.counter.Add(1)
		v, _ := t.dedupResult.LoadOrStore(pkv, atomic.NewInt64(0))
		v.(*atomic.Int64).Inc()
	}

	return nil
}

func (t *DedupTask) Summary() {
	total := t.counter.Load()
	fmt.Printf("%d duplicated entries found\n", total)
	var i int64
	var dumpFile *os.File
	var err error
	progressDisplay := uilive.New()
	progressFmt := "Dumping result ... %d%%(%d/%d)\n"
	var lastProgress int64
	if t.dumpFile != "" {
		dumpFile, err = os.Create(t.dumpFile)
		if err != nil {
			fmt.Println("failed to open dump file: ", err.Error())
		}
		defer dumpFile.Close()
		fmt.Println("Opening dump file: ", t.dumpFile)

		progressDisplay.Start()
		defer progressDisplay.Stop()
		fmt.Fprintf(progressDisplay, progressFmt, 0, 0, total)
	}

	t.dedupResult.Range(func(pk, cnt any) bool {
		if dumpFile != nil {
			dumpFile.WriteString(fmt.Sprintf("%v\n", pk))
			i++
			progress := i * 100 / total
			if progress > lastProgress {
				fmt.Fprintf(progressDisplay, progressFmt, progress, i, total)
				lastProgress = progress
			}
		} else {
			if i > 10 {
				return false
			}
			fmt.Printf("PK[%s] %v duplicated %d times\n", t.pkField.Name, pk, cnt.(*atomic.Int64).Load()+1)
			i++
		}
		return true
	})
}

func NewDedupTask(limit int64, pkField models.FieldSchema, dumpFile string) *DedupTask {
	return &DedupTask{
		limit:    limit,
		pkField:  pkField,
		dumpFile: dumpFile,
	}
}
