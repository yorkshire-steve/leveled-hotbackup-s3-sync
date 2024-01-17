# leveled-hotbackup-s3-sync
A Python 3.8 tool which can backup and restore leveled hotbackups with Amazon S3

## Quick Start
```
python3.8 -m venv .venv
source .venv/bin/activate
pip install leveled-hotbackup-s3-sync

# Usage
python -m leveled_hotbackup_s3_sync [backup|restore] [tag] --config config.cfg

[backup|restore] - specify operation to person
[tag] - alphanumeric tag to create backup, or to select which backup to restore from
--config config.cfg - filename for the config file, see examples/config.cfg

# Backup example
python -m leveled_hotbackup_s3_sync backup 123 --config config.cfg

# Restore example
python -m leveled_hotbackup_s3_sync restore 123 --config.cfg
```

## About
This tool will backup and restore LevelEd (https://github.com/martinsumner/leveled) hotbackups to/from Amazon S3.

Intended to be used with Riak KV.

Backup will use the local Riak ring data at `ring_path` to determine which partitions are owned by the local node, then upload each hotbackup from the `hotbackup_path` to `s3_path`. When backup is being uploaded to S3, the manifest files are updated to reference the new S3 URIs for the journal files. Specifiy a unique `tag` for each backup (this is then used by restore).

`hints_files = true` option in config will also create a hints file for every journal file. The hints file is a CDB of Bucket/Key to sequence number. 
The hints file is intended for future functionality of single object retrieval directly from S3 where bucket/key are known but sequence number is not.

To restore to local filesystem, the same `tag` used during backup must be used.
Restore will use the local Riak ring data at `ring_path` to determine which partitions are owned by the local node. Restore will then download the relevant tagged manifest from `s3_path` to the local `leveled_path` and download journal files if needed.
New manifests are then written locally with updated references to the new journal file locations.

## Testing
To aid testing a `docker-compose.yml` is included in the `localstack` directory which will start Localstack to use as a local S3 service.
```
make localstack
```

The `s3_endpoint` config parameter can then be passed to the python script to use a different S3 endpoint URL.
```
python -m leveled_hotbackup_s3_sync -l /local/path/to/leveled -s s3://bucket/hotbackup/ -e http://localhost:4566
```

## Riak backup example
To perform a Riak LevelEd hotbackup.
```
sudo riak remote_console
{ok, C} = riak:local_client().
riak_client:hotbackup("/path/to/backup",3,3,C).
```
