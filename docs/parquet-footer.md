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
