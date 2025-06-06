package repair

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// DiskAnnIndexParamsCommand return repair segment command.
func DiskAnnIndexParamsCommand(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "diskann_index_params",
		Aliases: []string{"diskann_index_params"},
		Short:   "check index parma and try to repair",
		Run: func(cmd *cobra.Command, args []string) {
			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			run, err := cmd.Flags().GetBool("run")
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			indexes, err := common.ListIndex(context.TODO(), cli, basePath, func(index *models.FieldIndex) bool {
				return collID == 0 || index.GetProto().GetIndexInfo().GetCollectionID() == collID
			})
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			DISKANNParamsMap := map[string]struct{}{
				"max_degree":                   {},
				"search_list_size":             {},
				"pq_code_budget_gb":            {},
				"build_dram_budget_gb":         {},
				"disk_pq_dims":                 {},
				"partition_limit":              {},
				"accelerate_build":             {},
				"search_cache_budget_gb":       {},
				"warm_up":                      {},
				"use_bfs_cache":                {},
				"beamwidth":                    {},
				"min_k":                        {},
				"max_k":                        {},
				"search_list_and_k_ratio":      {},
				"filter_threshold":             {},
				"pq_code_budget_gb_ratio":      {},
				"num_build_thread_ratio":       {},
				"search_cache_budget_gb_ratio": {},
				"num_load_thread_ratio":        {},
				"beamwidth_ratio":              {},
			}
			shareParamsMap := map[string]struct{}{
				"index_type":        {},
				"metric_type":       {},
				"k":                 {},
				"num_build_thread":  {},
				"retrieve_friendly": {},
				"data_path":         {},
				"index_prefix":      {},
				"build_quant_type":  {},
				"search_quant_type": {},
				"radius":            {},
				"range_filter":      {},
				"trace_visit":       {},
				"enable_mmap":       {},
				"for_tuning":        {},
			}
			HNSWParamsMap := map[string]struct{}{
				"M":               {},
				"efConstruction":  {},
				"ef":              {},
				"seed_ef":         {},
				"overview_levels": {},
			}
			newIndexes := make([]*models.FieldIndex, 0)
			unnecessaryParamsMap := make(map[int64][]string, 0)
			for _, info := range indexes {
				index := info.GetProto()

				newIndex := &indexpb.FieldIndex{
					IndexInfo: &indexpb.IndexInfo{
						CollectionID:         index.GetIndexInfo().GetCollectionID(),
						FieldID:              index.GetIndexInfo().GetFieldID(),
						IndexName:            index.GetIndexInfo().GetIndexName(),
						IndexID:              index.GetIndexInfo().GetIndexID(),
						TypeParams:           index.GetIndexInfo().GetTypeParams(),
						IndexParams:          make([]*commonpb.KeyValuePair, 0),
						IndexedRows:          index.GetIndexInfo().GetIndexedRows(),
						TotalRows:            index.GetIndexInfo().GetTotalRows(),
						State:                index.GetIndexInfo().GetState(),
						IndexStateFailReason: index.GetIndexInfo().GetIndexStateFailReason(),
						IsAutoIndex:          index.GetIndexInfo().GetIsAutoIndex(),
						UserIndexParams:      index.GetIndexInfo().GetUserIndexParams(),
					},
					Deleted:    index.GetDeleted(),
					CreateTime: index.GetCreateTime(),
				}
				indexType := ""
				for _, pair := range index.IndexInfo.IndexParams {
					if pair.Key == "index_type" {
						indexType = pair.Value
					}
				}
				if indexType != "DISKANN" && indexType != "HNSW" {
					continue
				}
				unnecessaryParams := make([]string, 0)
				if indexType == "DISKANN" {
					for _, pair := range index.IndexInfo.IndexParams {
						if _, ok := DISKANNParamsMap[pair.Key]; !ok {
							if _, ok2 := shareParamsMap[pair.Key]; !ok2 {
								unnecessaryParams = append(unnecessaryParams, pair.Key)
								continue
							}
						}
						newIndex.IndexInfo.IndexParams = append(newIndex.IndexInfo.IndexParams, pair)
					}
				} else if indexType == "HNSW" {
					for _, pair := range index.IndexInfo.IndexParams {
						if _, ok := HNSWParamsMap[pair.Key]; !ok {
							if _, ok2 := shareParamsMap[pair.Key]; !ok2 {
								unnecessaryParams = append(unnecessaryParams, pair.Key)
								continue
							}
						}
						newIndex.IndexInfo.IndexParams = append(newIndex.IndexInfo.IndexParams, pair)
					}
				}

				if len(unnecessaryParams) != 0 {
					unnecessaryParamsMap[newIndex.IndexInfo.IndexID] = unnecessaryParams
					newIndexes = append(newIndexes, models.NewProtoWrapper(newIndex, info.Key()))
				}
			}
			if !run {
				fmt.Println("has unnecessary params index:")
				fmt.Println(unnecessaryParamsMap)
				fmt.Println("after repair index:")
				for _, index := range newIndexes {
					printIndexV2(index.GetProto())
				}
				return
			}
			for _, index := range newIndexes {
				if err := writeRepairedIndex(cli, basePath, index.GetProto()); err != nil {
					fmt.Println(err.Error())
					return
				}
			}
			afterRepairIndexes, err := common.ListIndex(context.TODO(), cli, basePath)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			for _, index := range afterRepairIndexes {
				printIndexV2(index.GetProto())
			}
		},
	}

	cmd.Flags().Int64("collection", 0, "collection id to filter with")
	cmd.Flags().Bool("run", false, "actual do repair")
	return cmd
}
