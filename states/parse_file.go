package states

import (
	"encoding/json"
	"fmt"

	"github.com/milvus-io/birdwatcher/storage"
	"github.com/spf13/cobra"
)

func GetParseIndexParamCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "parse-indexparam [file]",
		Short: "parse index params",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("should provide only one file path")
				return
			}
			f, err := openBackupFile(args[0])
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			defer f.Close()

			r, evt, err := storage.NewIndexReader(f)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			extra := make(map[string]any)
			json.Unmarshal(evt.ExtraBytes, &extra)

			if extra["key"].(string) != "indexParams" {
				fmt.Println("index data file found", evt.ExtraBytes)
				return
			}
			data, err := r.NextEventReader(f, evt.PayloadDataType)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			if len(data) != 1 {
				fmt.Println("event data length is not 1")
				return
			}

			params := make(map[string]string)
			json.Unmarshal(data[0], &params)
			fmt.Println(params)
		},
	}
	return cmd
}
