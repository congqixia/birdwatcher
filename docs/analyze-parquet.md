# Segment Parquet size analysis

After connecting to a Milvus instance:

```text
analyze parquet --segment 67890
analyze parquet --segment 67890 --file data.parquet
analyze parquet --segment 67890 --file data.parquet --pages
analyze parquet --segment 67890 --pages --rowGroups --top 20
analyze parquet --segment 67890 --pages --json
```

`--file` exactly matches a manifest file path or resolved object key; it is not a
substring, basename search, or arbitrary local file. Omitting it analyzes all
supported data files. Repeated references to the same backend/object key count
once within the segment. A physical file shared by multiple manifest row ranges
is measured in full once, not prorated to those ranges. File row counts describe
the physical file and must not be summed to infer segment rows: column groups
can store the same logical rows in separate files.

The command uses Birdwatcher's reflective command registration, the parser used
by `show manifest`, and `ManifestPathResolver` for mixed internal/external data.
Plain storage-version-2 insert logs without a manifest are also supported.
Other legacy binlog layouts, encrypted Parquet and column chunks stored in
separate files are explicitly unsupported. Delta logs, stats, Milvus indexes
and LOB files are outside the report. Parquet's own page indexes are included.

## Metadata analysis

Default mode reads the four-byte file header, eight-byte trailer, and serialized
footer metadata. It does not read data pages. When a Bloom filter has an offset
but no length, it additionally reads its small Compact Protocol header to obtain
the bitset length. Failure to determine this length is reported; those bytes
remain unclassified. Metadata reads can include a few adjacent bytes.

The disjoint storage composition contains:

- `file_header`: first four bytes (`PAR1`).
- `column_chunks`: compressed pages including their page headers.
- `column_indexes` and `offset_indexes`: ranges recorded in footer metadata.
- `bloom_filters`: headers plus bitsets, where their size can be determined.
- `footer_metadata`: serialized Thrift FileMetaData.
- `footer_trailer`: four-byte metadata length and four-byte `PAR1` magic.
- `unclassified`: remaining gaps/padding or unsupported structures.

All known intervals are checked for bounds and overlaps. Invalid or overlapping
ranges fail that file, rather than producing an apparently balanced report.
Successful composition totals equal the Stat file size. Gaps alone do not imply
corruption. Index contents, checksums, data payload integrity, and Bloom filter
algorithms are not validated by this size analysis.

Footer composition excludes the trailer and uses **original serialized byte
spans**, with no parse/reserialize size estimation:

- `schema`: schema field header and its whole collection.
- `key_value_metadata`: complete file-level metadata field and collection.
- `column_statistics`: Statistics field headers and values in column metadata.
- `column_metadata_other`: remaining ColumnChunk bytes (including column-level
  key-value metadata, encodings, offsets, sizes and other fields).
- `row_group_overhead`: row-group structure, columns-list headers and remaining
  row-group fields.
- `other`: remaining file fields, unknown fields, field headers and stop markers.

These categories sum exactly to serialized metadata length. Per-column footer
bytes include the entire ColumnChunk struct, including its statistics, but not
the enclosing columns-list/row-group overhead. Per-column min/max value bytes
sum raw byte lengths of both legacy min/max and newer min_value/max_value fields
when present; they are a subset of statistics bytes and are not added again.

Key-value output prints keys and value lengths, never metadata values. Entry
serialized sizes include each entry's struct encoding; enclosing list/field
headers remain in the overall key-value category.

The report includes per-file and per-row-group physical column size, physical
type, codec, encoding list, value count, footer/statistics size and min/max value
lengths. Columns are ranked both by compressed size and footer size. Row-group
column details are always in JSON; use `--rowGroups` to print them in text.
`--top` limits text rankings, not JSON detail.

## Page analysis

`--pages` walks each column chunk by parsing Compact Protocol page headers and
advancing by the recorded compressed payload length. It never decompresses or
materializes values. It distinguishes DATA_PAGE, DATA_PAGE_V2, DICTIONARY_PAGE,
INDEX_PAGE and unknown future page types. It records page counts, serialized
header bytes, compressed payload bytes, and uncompressed payload bytes for each
type, per row-group column and aggregated per physical column.

A 64 KiB read-ahead cache combines nearby headers. Large payloads are skipped by
advancing offsets. Read-ahead can fetch adjacent payload bytes; this is not a
promise of zero payload network traffic. Tiny pages or pages with very large
headers can cause many requests. Each header is limited to 16 MiB. Headers that
extend beyond a chunk or payloads exceeding a chunk fail the file. Page header
plus compressed payload totals must equal the column-chunk compressed size.
Uncompressed page totals are compared with footer metadata; mismatches are
reported as findings. CRC and payload decoding are outside scope.

## Units and interpretation

Text uses decimal units, 1 KB = 1000 bytes. JSON retains byte counts. Footer size
includes the eight-byte trailer; footer composition describes only metadata.
Uncompressed encoded bytes come from Parquet metadata: they are neither original
business data size nor decoded heap usage. Compression ratio is uncompressed
encoded bytes divided by compressed bytes, including page headers on both sides.

Findings link measured evidence to investigations, not unconditional tuning:
statistics occupying over half of footer metadata, oversized key-value metadata,
more than 100 row groups averaging fewer than 1000 rows, dictionary pages larger
than data pages, unclassified ranges, or metadata/page inconsistencies. Evaluate
writer changes against pruning, memory and query performance.

Segment summary and rankings include successful files only. Per-file failures
and discovery errors remain visible in text and JSON; a partial report does not
claim full segment coverage. File failures do not stop analysis of other files.
Context cancellation stops the command.

## Limits and OSS overrides

`--maxFooterBytes` defaults to 134217728 (128 MiB) per file. The Compact Protocol
scanner also limits nesting to 64 and nodes to 2 million. Files are processed
sequentially and metadata is parsed in memory; decoded structures can consume
more memory than serialized footer bytes. Increase the byte limit deliberately
for larger footers. Per-file results retain column/row-group metrics for JSON.

The internal OSS override flags are shared with `show parquet-footer` and
`show manifest`; see [parquet-footer.md](parquet-footer.md). For masked gRPC
configuration, for example:

```text
analyze parquet --segment 67890 --pages --ossStandalone --minioAddr s3.amazonaws.com --cloudProvider aws --bucketName my-bucket --rootPath files --region us-east-1 --useIAM true --useSSL true
```

External credentials are resolved from collection schema only when a selected
external object needs to be read. The external location may be parsed to match a
resolved key even when only an internal file is selected.
