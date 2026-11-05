# Birdwatcher

[![linter status](https://github.com/milvus-io/birdwatcher/workflows/golangci-lint/badge.svg)](https://github.com/milvus-io/birdwatcher/actions/workflows/golangci-lint.yml?query=branch%3Amain+)

A debug tool for Milvus 2.0 or later. This tool connects to etcd and inspects some status of the Milvus system.

## Get Started

### Prerequisite
go 1.18 or higher 

### Install birdwatcher

download source and run with go command

```shell
git clone https://github.com/milvus-io/birdwatcher
cd birdwatcher
go install
```

tips: [As JimB notice in comments](https://stackoverflow.com/questions/69807151/go-install-github-com-dmacvicar-terraform-provider-libvirtlatest-shows-error):

> If there are replace or exclude directives in the module, the correct installation method is to clone the source and install it.

## How to use

### connect to etcd 

<img alt="connect to etcd" src="./docs/images/connect.gif" width="600" />

```shell
❯ birdwatcher
# execute connect command, using default config, equal to connect --etcd localhost:2379 --rootPath by-dev
Offline: connect 
Using meta path: by-dev/meta/
✔ Milvus(by-dev): show collection
...
```

### connect to remote instance etcd with other basePath

```shell
❯ birdwatcher
Offline: connect --etcd [ip]:[port] --rootPath instanceName
Using meta path: instanceName/meta/
✔ Milvus(instanceName): 

```

### inspect some meta


<img alt="inspect meta" src="./docs/images/show.gif" width="600" />

```
Milvus(by-dev): show collections
================================================================================
Collection ID: 434393323906469185	Collection Name:gosdk_index_example
Partitions:
 - Partition ID:434393323906469186	Partition Name:_default
Fields:
 - Field ID: 0 	 Field Name: RowID 	 Field Type: Int64
 - Field ID: 1 	 Field Name: Timestamp 	 Field Type: Int64
 - Field ID: 100 	 Field Name: ID 	 Field Type: Int64
	 - Primary Key, AutoID: false
 - Field ID: 101 	 Field Name: Year 	 Field Type: Int32
 - Field ID: 102 	 Field Name: Vector 	 Field Type: FloatVector
	 - Type Param dim: 8
Consistency Level: Strong
✔ Milvus(by-dev): █
```

### backup etcd


<img alt="inspect meta" src="./docs/images/backup.gif" width="600" />

```
Milvus(by-dev): backup
found 37 keys, at revision 533816, starting backup...
Backing up ... 100%(37/37)
backup etcd for prefix by-dev/meta done, stored in file: bw_etcd_ALL.220707-152246.bak.gz
```

### OSS path completion

After `connect oss`, `cd` suggests paths by listing object storage prefixes.
For buckets with many prefixes, disable these suggestions with:

```shell
set bw-config --key OSS_PATH_COMPLETION --value false
```

Completion is enabled by default. Set the value to `true` (or an empty value)
to enable it again. The setting takes effect immediately and uses the process
environment, so it can also be set before starting Birdwatcher.

### help

And use `help` command to check other commands.

### repair segment index types after snapshot restore

When restored etcd segment index records have a missing or incorrect actual index
type, the operator can explicitly set it without rebuilding the index files:

```text
repair segment-index-type --collection 123 --index 456 --index-type HNSW
repair segment-index-type --collection 123 --index 456 --index-type HNSW --run
```

The first command previews the changes. `--run` reads and validates a fresh plan,
then updates each selected record with compare-and-swap and verifies readback.
If a write fails, it stops and reports the number of confirmed commits without
rolling them back. Rerunning builds a fresh plan and skips already-correct types.
The operator must verify that the existing index artifacts actually use
the specified type; this command does not inspect files or infer types from the
collection definition. For mixed types, use `--segment <id>` to select one segment
per invocation. `AUTOINDEX` is not a concrete artifact type.

Only non-deleted, finished indexes with files on live, Flushed segments are
updated. Other protobuf fields, including unknown fields, are preserved. Matching
types are a no-op. Manifest-owned indexes are unsupported: any selected live
segment marked as containing manifest indexes causes the entire operation to fail,
even if that segment has no etcd index row. Segment-rooted or unknown index path
versions are also rejected. No manifest or collection index definition is changed.

Use a live etcd connection (normal audit wrappers are supported; TiKV and offline
backups are unsupported). Back up metadata and stop metadata writers, including
all eligible DataCoord leaders, before applying the repair. Concurrent changes
to an index record, its segment or its index definition stop that record from
being overwritten. New records created after preflight are outside the plan.
There is no collection-wide transaction or automatic retry. A timeout can leave
the current write outcome unknown: inspect that key before retrying. After a
successful repair, restart DataCoord and reload the affected collection before
serving queries. Updating etcd alone does not refresh running components' caches.
