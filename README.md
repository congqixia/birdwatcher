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
