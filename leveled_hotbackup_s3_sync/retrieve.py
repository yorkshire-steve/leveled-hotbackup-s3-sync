import argparse
from datetime import datetime
from typing import Union

import cdblib

from leveled_hotbackup_s3_sync.hints import get_sqn
from leveled_hotbackup_s3_sync.journal import decode_journal_object
from leveled_hotbackup_s3_sync.manifest import get_partition_manifest, guess_s3_ringsize
from leveled_hotbackup_s3_sync.s3mmap import S3FileReader
from leveled_hotbackup_s3_sync.utils import (
    RiakObject,
    check_endpoint_url,
    create_journal_key,
    find_primary_partition,
    guess_local_ringsize,
    is_s3_url,
    str_to_bytes,
)


def get_cdb_reader(filename: str, endpoint: Union[str, None] = None) -> cdblib.Reader:
    if is_s3_url(filename):
        s3_file = S3FileReader(filename, endpoint)
        return cdblib.Reader(data=s3_file)
    return cdblib.Reader.from_file_path(filename)


def find_sqn(
    manifest: list,
    bucket: bytes,
    bkey: bytes,
    buckettype: Union[bytes, None] = None,
    endpoint: Union[str, None] = None,
) -> tuple:
    sqn = None
    for journal in manifest:
        hints_filename = f"{journal[1].decode('utf-8')}.hints.cdb"
        journal_filename = f"{journal[1].decode('utf-8')}.cdb"
        print(f"Checking {hints_filename}")
        with get_cdb_reader(hints_filename, endpoint) as reader:
            sqn = get_sqn(reader, bucket, bkey, buckettype)
        if sqn:
            return sqn, journal_filename
    return None, None


def find_object(journal_filename: str, journal_key: bytes, endpoint: Union[str, None] = None) -> RiakObject:
    riak_object = RiakObject()
    with get_cdb_reader(journal_filename, endpoint) as reader:
        obj = reader.get(journal_key)
    if obj:
        journal_object = decode_journal_object(journal_key, obj)
        riak_object.decode(journal_object)
    return riak_object


def retrieve_object(args: argparse.Namespace) -> None:
    ringsize = args.ringsize
    if not ringsize:
        if is_s3_url(args.location):
            ringsize = guess_s3_ringsize(args.location, args.version, args.endpoint)
        else:
            ringsize = guess_local_ringsize(args.location)
        print(f"No ring size provided. Detected ring size of {ringsize}.\n")

    partition = find_primary_partition(ringsize, args.bucket, args.key, args.buckettype)
    print(f"Primary partition for given bucket/key is {partition}\n")

    partition_manifest = get_partition_manifest(args.location, partition, args.version, args.endpoint)
    print(f"Loaded partition manifest, {len(partition_manifest)} possible journal files to check\n")

    sqn, journal_filename = find_sqn(partition_manifest, args.bucket, args.key, args.buckettype, args.endpoint)

    if sqn:
        print(f"Found SQN {sqn} for journal {journal_filename}\n")
        journal_key = create_journal_key(sqn, args.bucket, args.key, args.buckettype)
        riak_object = find_object(journal_filename, journal_key, args.endpoint)
        num_siblings = len(riak_object.siblings)
        if num_siblings == 0:
            print(f"Could not find bucket/key in {journal_filename}\n")
        elif num_siblings == 1:
            print("Found object in journal.")
            print_sibling(riak_object.siblings[0])
        else:
            print(f"Found {num_siblings} siblings.\n")
            for idx, sibling in enumerate(riak_object.siblings):
                print(f"Sibling {idx}:")
                print_sibling(sibling)


def print_sibling(sibling: dict) -> None:
    last_modified = datetime.fromtimestamp(float(sibling["metadata"]["last_modified"]))
    vtag = sibling["metadata"]["vtag"].decode("utf-8")
    print(f"Last Modified: {last_modified}. Vtag: {vtag}\n")
    print("Object value:\n\n", sibling["value"], "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="Riak Backup Retrieve",
        description="Retrieve a single object from Riak hot-backup",
    )
    parser.add_argument(
        "-l",
        "--location",
        type=str,
        required=True,
        help="Location, either local directory (/path/to/dir) or S3 URI (s3://bucket/path)",
    )
    parser.add_argument("-b", "--bucket", type=str_to_bytes, required=True, help="Bucket")
    parser.add_argument("-k", "--key", type=str_to_bytes, required=True, help="Key")
    parser.add_argument("-t", "--buckettype", type=str_to_bytes, required=False, help="Bucket Type")
    parser.add_argument("-r", "--ringsize", type=int, required=False, default=None, help="Riak ring size")
    parser.add_argument(
        "-e",
        "--endpoint",
        type=check_endpoint_url,
        required=False,
        default=None,
        help="S3 Endpoint URL to override AWS default",
    )
    parser.add_argument(
        "-v",
        "--version",
        required=False,
        default=None,
        help="S3 VersionId of MANIFESTS to restore from",
    )
    args = parser.parse_args()

    if is_s3_url(args.location) and not args.version:
        raise ValueError("Need --version when location is S3")

    retrieve_object(args)
