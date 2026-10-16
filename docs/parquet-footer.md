# Collection Parquet footer analysis

After connecting to an instance, run:

```text
show parquet-footer --collection 12345
show parquet-footer --collection 12345 --segment 67890 --top 20
show parquet-footer --collection 12345 --json
```

The command follows the reflective command registration described in
`bw_framework.md`. It lists non-dropped segments from etcd and uses the same
manifest parser as `show manifest`. Manifest column-group paths are resolved
through `ossutil.ManifestPathResolver`, including mixed internal/external storage.
Storage version 2 segments without manifests use insert-binlog paths. Other
segments without manifests are explicitly skipped. Delta logs, indexes, stats
and LOB files are outside this report.

Each data object requires a Stat request and range reads of the first four and
last eight bytes. No Parquet data pages or serialized metadata are downloaded.
Only plain PAR1 Parquet is supported; encrypted or invalid objects are reported
as failures. Metadata bytes are the little-endian length from the trailer;
footer bytes include that metadata plus the eight-byte length/magic trailer.
This measures serialized storage size, not decoded metadata memory usage.

Text output contains collection and segment totals, footer/file percentage,
minimum, mean, nearest-rank p50/p95, maximum and the largest footers. JSON includes
all file results and errors. Percentages use total footer bytes divided by total
file bytes for successful objects. Failed files are excluded from statistics.
Duplicate object references within a segment count once; shared objects across
segments count once for each segment, reflecting segment-level footer overhead.
A high percentage indicates relatively large metadata overhead; a high p95/max
relative to p50 identifies files worth inspecting with `inspect-parquet` for
schema and row-group details. This command does not infer the cause of large
footers from their size alone.

File/manifest failures produce a partial report with explicit errors. Check those
and skipped segment counts before treating results as collection-wide totals.
Use `--minioAddr` and `--skipBucketCheck` as with `show manifest`; external object
credentials come from the collection schema.

## Overriding masked component configuration

Both `show parquet-footer` and `show manifest` accept internal object store
connection overrides: `--minioAddr`, `--port`, `--cloudProvider`, `--region`,
`--bucketName`, `--rootPath`, `--accessKeyID`, `--secretAccessKey`, `--useIAM`,
`--useSSL`, `--iamEndpoint`, `--roleARN`, `--roleSessionName`, `--externalID`,
`--aliyunRoleAuthMode`, and `--loadFrequency` (credential reload seconds).
Omitted flags preserve discovered configuration. Supply optional boolean values
as `--useIAM true` or `--useSSL false`; these flags require a value.

When high-version components mask configuration, use `--ossStandalone` to skip
configuration discovery entirely. In this mode `--minioAddr`, `--cloudProvider`
and `--bucketName` are required; omitted TLS/IAM values default to false, and
port/root path/role fields default to empty. Specify the real root path when
manifest references contain `ROOT_PATH`.

```text
show parquet-footer --collection 12345 --ossStandalone --minioAddr s3.amazonaws.com --cloudProvider aws --bucketName my-bucket --rootPath files --region us-east-1 --useIAM true --useSSL true
show manifest --collection 12345 --ossStandalone --minioAddr localhost:9000 --cloudProvider aws --bucketName milvus --rootPath files --useIAM false --useSSL false --accessKeyID YOUR_AK --secretAccessKey YOUR_SK
```

Overrides affect internal storage. External data object credentials continue to
come from the collection schema. Standalone mode also avoids inheriting masked
role credentials, which would otherwise take precedence over IAM/static auth.
