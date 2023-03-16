package states

import (
	"encoding/json"
	"fmt"
	"path"

	"github.com/cockroachdb/errors"
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
			key := extra["key"].(string)
			if key != "indexParams" && key != "SLICE_META" {
				fmt.Println("index data file found", extra)
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

			switch key {
			case "indexParams":
				params := make(map[string]string)
				json.Unmarshal(data[0], &params)
				fmt.Println(params)
			case "SLICE_META":
				fmt.Println(string(data[0]))
			}

		},
	}
	return cmd
}

func GetOrganizeIndexFilesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use: "organize-indexfiles",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				fmt.Println("should provide only one file path")
				return
			}

			folder := args[0]
			if err := testFolder(folder); err != nil {
				fmt.Println(err.Error())
				return
			}

			sliceMetaFile := path.Join(folder, "SLICE_META")
			prefix, num, err := tryParseSliceMeta(sliceMetaFile)
			if err != nil {
				fmt.Println("failed to parse SLICE_META", err.Error())
				return
			}

			fmt.Println(prefix, num)

		},
	}
	return cmd
}

func tryParseSliceMeta(file string) (string, int, error) {
	if err := testFile(file); err != nil {
		fmt.Println("failed to test SLICE_META")
		return "", 0, err
	}

	f, err := openBackupFile(file)
	if err != nil {
		fmt.Println(err.Error())
		return "", 0, err
	}
	defer f.Close()

	r, evt, err := storage.NewIndexReader(f)
	if err != nil {
		fmt.Println(err.Error())
		return "", 0, err
	}
	extra := make(map[string]any)
	json.Unmarshal(evt.ExtraBytes, &extra)
	key := extra["key"].(string)
	if key != "SLICE_META" {
		fmt.Println("file meta shows file not SLICE_META, but", key)
		return "", 0, errors.New("file not SLICE_META")
	}

	data, err := r.NextEventReader(f, evt.PayloadDataType)
	if err != nil {
		fmt.Println(err.Error())
		return "", 0, err
	}
	meta := &SliceMeta{}
	err = json.Unmarshal(data[0], meta)
	if err != nil {
		fmt.Println("failed to unmarsahl", err.Error())
		return "", 0, err
	}

	if len(meta.Meta) != 1 {
		return "", 0, errors.Newf("slice_meta item is not 1 but %d", len(meta.Meta))
	}

	fmt.Println("total_num", meta.Meta[0].TotalLength)
	return meta.Meta[0].Name, meta.Meta[0].SliceNum, nil
}

type SliceMeta struct {
	Meta []struct {
		Name        string `json:"name"`
		SliceNum    int    `json:"slice_num"`
		TotalLength int64  `json:"total_len"`
	} `json:"meta"`
}
