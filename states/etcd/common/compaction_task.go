package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// ListCompactionTask returns compaction task information as provided filters.
func ListCompactionTask(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(task *models.CompactionTask) bool) ([]*models.CompactionTask, error) {
	prefix := path.Join(basePath, DCPrefix, CompactionTaskPrefix) + "/"
	// return ListObj2Models(ctx, cli, prefix, models.NewCompactionTask, filters...)
	return WalkObjWithPrefix(ctx, cli, prefix, 100, 30, models.NewCompactionTask, filters...)
}
