package states

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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
		Use: "organize-indexfiles [file]",
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

			fmt.Printf("original file name: %s, slice num: %d\n", prefix, num)

			m := make(map[int64]struct{})

			filepath.Walk(folder, func(file string, info os.FileInfo, err error) error {
				file = path.Base(file)
				if !strings.HasPrefix(file, prefix+"_") {
					fmt.Println("skip file", file)
					return nil
				}

				suffix := file[len(prefix)+1:]
				idx, err := strconv.ParseInt(suffix, 10, 64)
				if err != nil {
					fmt.Println(err.Error())
					return nil
				}

				m[idx] = struct{}{}
				return nil
			})
			if len(m) != num {
				fmt.Println("slice files not complete", m)
				return
			}

			outputPath := fmt.Sprintf("%s_%s", prefix, time.Now().Format("060102150406"))
			output, err := os.OpenFile(outputPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			defer output.Close()
			totalLen := int64(0)

			for i := 0; i < num; i++ {
				key := fmt.Sprintf("%s_%d", prefix, i)
				fmt.Print("processing file:", key)
				data, err := readIndexFile(path.Join(folder, key), func(metaKey string) bool {
					return metaKey == key
				})
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				fmt.Println(" read data size:", len(data), hrSize(int64(len(data))))

				_, err = output.Write(data)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				totalLen += int64(len(data))
			}
			fmt.Printf("index file write to %s success, total len %d\n", outputPath, totalLen)
		},
	}
	return cmd
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

func tryParseSliceMeta(file string) (string, int, error) {
	data, err := readIndexFile(file, func(key string) bool {
		if key != "SLICE_META" {
			fmt.Println("failed meta indicates file content not SLICE_META but", key)
			return false
		}
		return true
	})
	meta := &SliceMeta{}
	raw := bytes.Trim(data, "\x00")
	err = json.Unmarshal(raw, meta)
	if err != nil {
		fmt.Println("failed to unmarshal", err.Error())
		return "", 0, err
	}

	if len(meta.Meta) != 1 {
		return "", 0, errors.Newf("slice_meta item is not 1 but %d", len(meta.Meta))
	}

	fmt.Printf("SLICE_META total_num parsed: %d\n", meta.Meta[0].TotalLength)
	return meta.Meta[0].Name, meta.Meta[0].SliceNum, nil
}

type SliceMeta struct {
	Meta []struct {
		Name        string `json:"name"`
		SliceNum    int    `json:"slice_num"`
		TotalLength int64  `json:"total_len"`
	} `json:"meta"`
}

func readIndexFile(file string, validKey func(key string) bool) ([]byte, error) {
	if err := testFile(file); err != nil {
		fmt.Println("failed to test file", file)
		return nil, err
	}

	f, err := openBackupFile(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r, evt, err := storage.NewIndexReader(f)
	if err != nil {
		return nil, err
	}
	extra := make(map[string]any)
	json.Unmarshal(evt.ExtraBytes, &extra)
	key := extra["key"].(string)
	if !validKey(key) {
		return nil, errors.New("file meta key not valid")
	}

	data, err := r.NextEventReader(f, evt.PayloadDataType)
	if err != nil {
		return nil, err
	}
	if len(data) != 1 {
		return nil, errors.Newf("index file suppose to contain only one block but got %d", len(data))
	}

	return data[0], nil
}
