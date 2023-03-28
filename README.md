# leveled-hotbackup-s3-sync
Python tool which can backup and restore leveled hotbackups with S3

## Quick Start
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Backup (default)
python app.py -l /local/path/to/leveled -s s3://bucket/hotbackup/

# List backups
python app.py -s s3://bucket/hotbackup/ -a list

# Restore
python app.py -l /local/path/to/restore/to -s s3://bucket/hotbackup/ -a restore -v VERSION_ID_STRING
```

## About
This tool will backup and restore LevelEd (https://github.com/martinsumner/leveled) to/from S3.

Intended to be used with Riak KV.

When backup is being uploaded to S3, the manifest files are updated to reference the new S3 URIs for the journal files.
A hints file is created for every journal file which is a CDB of Bucket/Key to sequence number.
The hints file is intended for future functionality of single object retrieval directly from S3 where bucket/key are known but sequence number is not.
A `MANIFESTS` is also created and uploaded to S3 which contains a list of all manifests and the VersionId of the S3 object to aid in point in time recovery.

A list of versions can be retrived with `-a list` argument which will print out the Last Modified timestamps and Version IDs of the `MANIFESTS` object in S3.

To restore to local filesystem, a Version ID from the list command is needed. Restore can be performed with `-a restore -v VERSIONID` argument.
This will read the `MANIFESTS` file to get the correct versions of all manifests, use these to determine journal files to download.
New manifests are then written locally with updated references to the new journal file locations.

## Testing
To aid testing a `docker-compose.yml` is included in the `localstack` directory which will start Localstack to use as a local S3 service.
```
cd localstack
docker compose up -d
```

The `--endpoint` or `-e` parameter can then be passed to the python script to use a different S3 endpoint URL.
```
python app.py -l /local/path/to/leveled -s s3://bucket/hotbackup/ -e http://localhost:4566
```

To perform a Riak hotbackup
```
sudo riak remote_console
{ok, C} = riak:local_client().
riak_client:hotbackup("/path/to/backup",3,3,C).
```
