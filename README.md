# leveled-hotbackup-s3-sync
A Python 3.8 tool which can backup and restore leveled hotbackups with Amazon S3

## Quick Start
```
python3.8 -m venv .venv
source .venv/bin/activate
pip install leveled-hotbackup-s3-sync

# Backup (default)
python -m leveled_hotbackup_s3_sync -l /local/path/to/leveled/hotbackup -s s3://bucket/hotbackup/

# List backups
python -m leveled_hotbackup_s3_sync -s s3://bucket/hotbackup/ -a list

# Restore (replace VERSION_ID_STRING with an ID from the above list command)
python -m leveled_hotbackup_s3_sync -l /local/path/to/restore/to -s s3://bucket/hotbackup/ -a restore -v VERSION_ID_STRING
```

## About
This tool will backup and restore LevelEd (https://github.com/martinsumner/leveled) hotbackups to/from Amazon S3.

Intended to be used with Riak KV.

The target Amazon S3 bucket must have versioning enabled.

When backup is being uploaded to S3, the manifest files are updated to reference the new S3 URIs for the journal files.
A `MANIFESTS` is also created and uploaded to S3 which contains a list of all manifests and their VersionIds to aid in point in time recovery.

`--hintsfiles` option will also create a hints file for every journal file. The hints file is a CDB of Bucket/Key to sequence number. 
The hints file is intended for future functionality of single object retrieval directly from S3 where bucket/key are known but sequence number is not.

A list of versions can be retrived with `-a list` argument which will print out the Last Modified timestamps and Version IDs of the `MANIFESTS` object in S3.

To restore to local filesystem, a Version ID from the list command is needed. Restore can be performed with `-a restore -v VERSIONID` argument.
This will read the given VersionId of the `MANIFESTS` file and get the correct versions of all manifests, then use these to determine the journal files to download.
New manifests are then written locally with updated references to the new journal file locations.

## Testing
To aid testing a `docker-compose.yml` is included in the `localstack` directory which will start Localstack to use as a local S3 service.
```
make localstack
```

The `--endpoint` or `-e` parameter can then be passed to the python script to use a different S3 endpoint URL.
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
