# leveled-hotbackup-s3-sync
A Python 3.8 tool which can backup and restore leveled hotbackups with Amazon S3, and optionally retrieve individual objects from a backup in Amazon S3.

## Quick Start
For backup and restore with S3.
```
python3.8 -m venv .venv
source .venv/bin/activate
pip install leveled-hotbackup-s3-sync

# Usage
s3sync [backup|restore] [tag] --config config.cfg

[backup|restore] - specify operation to perform
[tag] - alphanumeric string to tag backup, or to select which backup to restore from
--config config.cfg - filename for the config file, see example config.cfg below

# Backup example
s3sync backup 123 --config config.cfg

# Restore example
s3sync restore 123 --config config.cfg
```

To use the object retrieval functionality, the python package must be installed with extras, shown below.
```
python3.8 -m venv .venv
source .venv/bin/activate
pip install 'leveled-hotbackup-s3-sync[retrieve]'

# Usage
s3retrieve [tag] --config config.cfg --bucket bucketName --key keyName [--buckettype type] [--output filename]

[tag] - alphanumeric string to select which backup to retrive object from
--config config.cfg - filename for the config file, see example config.cfg below
--bucket - name of bucket to retrieve object from
--key - name of object key to retrieve
[--buckettype type] - optional, bucket type to retrieve object from
[--output filename] - optional, filename to write out the object. If omitted, object value will be printed to screen.
```

## About
This tool will backup and restore LevelEd (https://github.com/martinsumner/leveled) hotbackups to/from Amazon S3.

Intended to be used with Riak KV.

Backup will use the local Riak ring data at `ring_path` to determine which partitions are owned by the local node, then upload each hotbackup from the `hotbackup_path` to `s3_path`. When backup is being uploaded to S3, the manifest files are updated to reference the new S3 URIs for the journal files. Specifiy a unique `tag` for each backup (this is then used by restore).

`hints_files = true` option in config will also create a hints file for every journal file. The hints file is a CDB of Bucket/Key to sequence number. 
The hints file is required for single object retrieval directly from S3. If a backup has been saved to S3 without this option, then attempting to perform a single object retrieval will result in an error.

To restore to local filesystem, the same `tag` used during backup must be used.
Restore will use the local Riak ring data at `ring_path` to determine which partitions are owned by the local node. Restore will then download the relevant tagged manifest from `s3_path` to the local `leveled_path` and download journal files if needed.
New manifests are then written locally with updated references to the new journal file locations.

## Example config.cfg
```
# hotbackup_path is the local filesystem path to the
# directory containing LevelEd hotbackups.
# e.g. what is passed to riak_client:hotbackup()
hotbackup_path = "/riak/hotbackup"

# ring_path is the local filesystem path to the
# Riak ring data directory. This is used to determine partition ownership
# of the current node.
ring_path = "/riak/data/ring"

# leveled_path is the local filesystem path to the
# Riak leveled data directory. This is used during restore.
leveled_path = "/riak/data/leveled"

# s3_path is the S3 URI to store/retrive the hotbackup
s3_path = "s3://bucket-name/hotbackup/"

# hints_file when set to true will create a hints file
# for each journal file. This is only needed for object level
# retrieval from S3
# valid values: true|false
# Optional
hints_files = true

# s3_endpoint can be used when the target S3 service
# is not the standard AWS S3 (e.g. localstack)
# Optional. Omit to use standard AWS URLs
# s3_endpoint = "http://localhost:4566"
```

## Testing
To aid testing a `docker-compose.yml` is included in the `localstack` directory which will start Localstack to use as a local S3 service.
```
make localstack
```

The `s3_endpoint` config parameter can then be set in config.cfg to use the localstack S3 endpoint URL (e.g. "http://localhost:4566").

## Riak backup example
To perform a Riak LevelEd hotbackup.
```
sudo riak remote_console
{ok, C} = riak:local_client().
riak_client:hotbackup("/riak/hotbackup",3,3,C).
```
