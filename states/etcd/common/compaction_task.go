package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
)

// ListCompactionTask returns compaction task information as provided filters.
func ListCompactionTask(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(task *models.CompactionTask) bool) ([]*models.CompactionTask, int64, error) {
	prefix := path.Join(basePath, DCPrefix, CompactionTaskPrefix) + "/"
	total, err := cli.CountWithPrefix(ctx, prefix)
	if err != nil {
		return nil, -1, err
	}
	tasks, err := WalkObjWithPrefix(ctx, cli, prefix, 100, 30, models.NewCompactionTask, filters...)
	return tasks, total, err
}
