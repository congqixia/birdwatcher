package states

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/ossutil"
	storagemanifest "github.com/milvus-io/birdwatcher/storage/manifest"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

type ShowManifestParam struct {
	oss.OSSOverrideParam
	framework.ParamBase `use:"show manifest" desc:"parse and display manifest file from S3 for segments"`
	CollectionID        int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	SegmentID           int64  `name:"segment" default:"0" desc:"segment id to display"`
	MinioAddress        string `name:"minioAddr" desc:"override minio address"`
	SkipBucketCheck     bool   `name:"skipBucketCheck" default:"false" desc:"skip bucket exist check"`
	JSONOutput          bool   `name:"json" default:"false" desc:"output as JSON"`
}

func (s *InstanceState) ShowManifestCommand(ctx context.Context, p *ShowManifestParam) error {
	if p.CollectionID == 0 && p.SegmentID == 0 {
		return fmt.Errorf("at least one of --collection or --segment must be specified")
	}

	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(seg *models.Segment) bool {
		return (p.CollectionID == 0 || seg.CollectionID == p.CollectionID) &&
			(p.SegmentID == 0 || seg.ID == p.SegmentID) &&
			seg.State != commonpb.SegmentState_Dropped
	})
	if err != nil {
		return err
	}

	// Filter segments that have manifest paths
	var manifestSegments []*models.Segment
	for _, seg := range segments {
		if seg.GetManifestPath() != "" {
			manifestSegments = append(manifestSegments, seg)
		}
	}

	if len(manifestSegments) == 0 {
		fmt.Println("No segments with manifest path found")
		return nil
	}

	resolvedStore, err := oss.NewResolvedObjectStoreWithOverrides(ctx, p.OSSOverrideParam, p.MinioAddress, p.SkipBucketCheck, s.GetObjectStore)
	if err != nil {
		return fmt.Errorf("failed to create minio client: %w", err)
	}
	rootPath := resolvedStore.RootPath

	// External collections: parse the external source/spec into a location
	// WITHOUT connecting to the external bucket. show manifest only reads the
	// manifest from Milvus internal storage and uses the location to annotate
	// paths, so it must not fail when external credentials or connectivity are
	// broken. The external client is created lazily only by commands that
	// actually read external objects (scan-binlog, inspect-parquet).
	// Resolve the collection by --collection, or fall back to the first manifest
	// segment when only --segment is given.
	var externalLocation ossutil.ExternalSourceLocation
	var externalResolved bool
	collectionID := p.CollectionID
	if collectionID == 0 && len(manifestSegments) > 0 {
		collectionID = manifestSegments[0].CollectionID
	}
	if collectionID != 0 {
		collection, err := common.GetCollectionByIDVersion(ctx, s.client, s.basePath, collectionID)
		if err != nil {
			return err
		}
		if collection.GetProto().GetSchema().GetExternalSource() != "" {
			externalLocation, err = ossutil.ResolveExternalLocationFromCollection(collection)
			if err != nil {
				return fmt.Errorf("failed to parse external location: %w", err)
			}
			externalResolved = true
		}
	}

	for _, seg := range manifestSegments {
		rawManifest := seg.GetManifestPath()
		fmt.Printf("=== Segment %d (Collection: %d, Partition: %d) ===\n", seg.ID, seg.CollectionID, seg.PartitionID)
		fmt.Printf("Manifest Raw: %s\n", rawManifest)

		// ManifestPath is JSON: {"ver":2,"base_path":"files/insert_log/..."}
		// Actual file: {rootPath}/{base_path}/_metadata/manifest-{ver}.arvo
		var manifestRef struct {
			Ver      int    `json:"ver"`
			BasePath string `json:"base_path"`
		}
		if err := json.Unmarshal([]byte(rawManifest), &manifestRef); err != nil {
			fmt.Printf("Error parsing manifest path JSON: %v\n\n", err)
			continue
		}

		basePath := oss.ResolveObjectKey(rootPath, manifestRef.BasePath)
		manifestPath := path.Join(basePath, "_metadata", fmt.Sprintf("manifest-%d.avro", manifestRef.Ver))
		fmt.Printf("Manifest File: %s\n", manifestPath)

		obj, err := resolvedStore.Store.Open(ctx, manifestPath)
		if err != nil {
			fmt.Printf("Error getting object: %v\n\n", err)
			continue
		}

		manifest, err := parseManifest(obj)
		if err != nil {
			fmt.Printf("Error parsing manifest: %v\n\n", err)
			continue
		}

		switch {
		case p.JSONOutput:
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			if err := enc.Encode(manifest); err != nil {
				fmt.Printf("Error encoding JSON: %v\n", err)
			}
		case externalResolved && seg.CollectionID == collectionID:
			// Annotate paths with storage backend / object key. The resolver
			// only annotates; it never opens external objects, so no external
			// store is built here.
			resolver := ossutil.NewManifestPathResolver(resolvedStore.Store, basePath, nil, externalLocation)
			printManifest(manifest, resolver)
		default:
			printManifest(manifest, nil)
		}
		fmt.Println()
	}

	return nil
}

// Manifest decoding is shared with storage v3 segment readers.
type manifest = storagemanifest.Manifest

const manifestVersionV6 = storagemanifest.VersionV6

func parseManifest(r io.ReadSeeker) (*manifest, error) {
	return storagemanifest.Parse(r)
}

// printManifest prints manifest entries. When resolver is non-nil, each file
// path is annotated with the storage backend and resolved object key; otherwise
// the raw paths are printed as stored. All existing diagnostic output (column
// group metadata, stats metadata, V6 index details, index file keys, index
// properties) is preserved in both modes.
func printManifest(m *manifest, resolver *ossutil.ManifestPathResolver) {
	fmt.Printf("Format:  %s\n", m.Format)
	if m.Format == "legacy_milv" {
		fmt.Printf("Magic:   0x%08X (%q)\n", m.Magic, m.MagicStr)
	}
	fmt.Printf("Version: %d\n", m.Version)

	fmt.Printf("\nColumn Groups (%d):\n", len(m.ColumnGroups))
	for i, cg := range m.ColumnGroups {
		fmt.Printf("\n  --- Column Group #%d ---\n", i)
		fmt.Printf("  Format:  %s\n", cg.Format)
		fmt.Printf("  Columns: %v\n", cg.Columns)
		fmt.Printf("  Files (%d):\n", len(cg.Files))
		for j, f := range cg.Files {
			fmt.Printf("    [%d] Path: %s\n", j, f.Path)
			fmt.Printf("        Range: [%d, %d)\n", f.StartIndex, f.EndIndex)
			if resolver != nil {
				_, key, backend, err := resolver.Resolve(f.Path, "_data")
				if err != nil {
					fmt.Printf("        Resolve Error: %v\n", err)
				} else {
					fmt.Printf("        Backend: %s  Object Key: %s\n", backend, key)
				}
			}
			if len(f.Metadata) > 0 {
				fmt.Printf("        Metadata (%d bytes): %s\n", len(f.Metadata), hex.EncodeToString(f.Metadata))
			}
			if len(f.Properties) > 0 {
				fmt.Printf("        Properties:\n")
				for pk, pv := range f.Properties {
					fmt.Printf("          %s: %s\n", pk, pv)
				}
			}
		}
	}

	fmt.Printf("\nDelta Logs (%d):\n", len(m.DeltaLogs))
	for i, dl := range m.DeltaLogs {
		fmt.Printf("  [%d] Path: %s  Type: %s  NumEntries: %d", i, dl.Path, dl.Type, dl.NumEntries)
		if resolver != nil {
			_, key, backend, err := resolver.Resolve(dl.Path, "_delta")
			if err != nil {
				fmt.Printf("  (Resolve Error: %v)", err)
			} else {
				fmt.Printf("  Backend: %s  Object Key: %s", backend, key)
			}
		}
		fmt.Println()
	}

	if len(m.Stats) > 0 {
		fmt.Printf("\nStats (%d keys):\n", len(m.Stats))
		for k, stat := range m.Stats {
			fmt.Printf("  %s:\n", k)
			fmt.Printf("    Paths:\n")
			for _, p := range stat.Paths {
				if resolver != nil {
					_, key, backend, err := resolver.Resolve(p, "_stats")
					if err != nil {
						fmt.Printf("      - %s (Resolve Error: %v)\n", p, err)
					} else {
						fmt.Printf("      - %s  Backend: %s  Object Key: %s\n", p, backend, key)
					}
				} else {
					fmt.Printf("      - %s\n", p)
				}
			}
			if len(stat.Metadata) > 0 {
				fmt.Printf("    Metadata:\n")
				for mk, mv := range stat.Metadata {
					fmt.Printf("      %s: %s\n", mk, mv)
				}
			}
		}
	}

	if len(m.Indexes) > 0 {
		fmt.Printf("\nIndexes (%d):\n", len(m.Indexes))
		for i, idx := range m.Indexes {
			fmt.Printf("  [%d] Column: %s  Type: %s  Path: %s\n", i, idx.ColumnName, idx.IndexType, idx.Path)
			if resolver != nil && idx.Path != "" {
				_, key, backend, err := resolver.Resolve(idx.Path, "_index")
				if err != nil {
					fmt.Printf("      Path Resolve Error: %v\n", err)
				} else {
					fmt.Printf("      Backend: %s  Object Key: %s\n", backend, key)
				}
			}
			if m.Version == manifestVersionV6 {
				fmt.Printf("      Name: %s  FieldID: %d  IndexID: %d  BuildID: %d\n",
					idx.IndexName, idx.FieldID, idx.IndexID, idx.BuildID)
				fmt.Printf("      Version: %d  Rows: %d  SerializedSize: %d  MemSize: %d\n",
					idx.IndexVersion, idx.NumRows, idx.SerializedSize, idx.MemSize)
				fmt.Printf("      EngineVersions: index=%d scalar=%d storePath=%d\n",
					idx.CurrentIndexVersion, idx.CurrentScalarIndexVersion, idx.IndexStorePathVersion)
			}
			if len(idx.IndexFileKeys) > 0 {
				fmt.Printf("      Files: %v\n", idx.IndexFileKeys)
			}
			if len(idx.Properties) > 0 {
				fmt.Printf("      Properties:\n")
				for pk, pv := range idx.Properties {
					fmt.Printf("        %s: %s\n", pk, pv)
				}
			}
		}
	}

	if len(m.LobFiles) > 0 {
		fmt.Printf("\nLOB Files (%d):\n", len(m.LobFiles))
		for i, lob := range m.LobFiles {
			fmt.Printf("  [%d] Path: %s  FieldID: %d  Rows: %d/%d  Size: %d",
				i, lob.Path, lob.FieldID, lob.ValidRows, lob.TotalRows, lob.FileSizeBytes)
			if resolver != nil {
				_, key, backend, err := resolver.Resolve(lob.Path, "../lobs")
				if err != nil {
					fmt.Printf("  (Resolve Error: %v)", err)
				} else {
					fmt.Printf("  Backend: %s  Object Key: %s", backend, key)
				}
			}
			fmt.Println()
		}
	}
}
