import argparse
import os.path
import sys
from datetime import datetime
from typing import Union

import cdblib

from leveled_hotbackup_s3_sync.config import read_config
from leveled_hotbackup_s3_sync.hints import get_sqn
from leveled_hotbackup_s3_sync.journal import decode_journal_object
from leveled_hotbackup_s3_sync.manifest import read_manifest
from leveled_hotbackup_s3_sync.s3mmap import S3FileReader
from leveled_hotbackup_s3_sync.utils import (
    RiakObject,
    create_journal_key,
    find_primary_partition,
    get_ring_size,
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


def retrieve_object(config: dict) -> None:
    ringsize = get_ring_size(config["ring_filename"])
    partition = find_primary_partition(ringsize, config["bucket"], config["key"], config["buckettype"])
    print(f"Primary partition for given bucket/key is {partition}\n")

    manifest_s3_path = os.path.join(config["s3_path"], str(partition), f"journal/journal_manifest/{config['tag']}.man")
    partition_manifest = read_manifest(manifest_s3_path, config["s3_endpoint"])
    print(f"Loaded partition manifest, {len(partition_manifest)} possible journal files to check\n")

    sqn, journal_filename = find_sqn(
        partition_manifest, config["bucket"], config["key"], config["buckettype"], config["s3_endpoint"]
    )

    if sqn:
        print(f"Found SQN {sqn} for journal {journal_filename}\n")
        journal_key = create_journal_key(sqn, config["bucket"], config["key"], config["buckettype"])
        riak_object = find_object(journal_filename, journal_key, config["s3_endpoint"])
        num_siblings = len(riak_object.siblings)
        if num_siblings == 0:
            print(f"Could not find bucket/key in {journal_filename}\n")
        elif num_siblings == 1:
            print("Found object in journal.")
            if config["output"]:
                write_sibling(config["output"], riak_object.siblings[0])
            else:
                print_sibling(riak_object.siblings[0])
        else:
            print(f"Found {num_siblings} siblings.\n")
            for idx, sibling in enumerate(riak_object.siblings):
                if config["output"]:
                    write_sibling(f"{config['output']}.{idx}", sibling)
                else:
                    print(f"Sibling {idx}:")
                    print_sibling(sibling)
    else:
        print("Could not find key in hotbackup.")


def print_sibling(sibling: dict) -> None:
    last_modified = datetime.fromtimestamp(float(sibling["metadata"]["last_modified"]))
    vtag = sibling["metadata"]["vtag"].decode("utf-8")
    print(f"Last Modified: {last_modified}. Vtag: {vtag}\n")
    print("Object value:\n\n", sibling["value"], "\n", sep="")


def write_sibling(filename: str, sibling: dict) -> None:
    print(f"Writing object to file {filename}")
    with open(filename, "wb") as file_handle:
        file_handle.write(sibling["value"])


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="Riak HotBackup Retrieve",
        description="Retrieve a single object from Riak hot-backup",
    )
    parser.add_argument("-b", "--bucket", type=str_to_bytes, required=True, help="Bucket")
    parser.add_argument("-k", "--key", type=str_to_bytes, required=True, help="Key")
    parser.add_argument("-t", "--buckettype", type=str_to_bytes, required=False, help="Bucket Type")
    parser.add_argument("-o", "--output", type=str, required=False, help="Output Filename")
    parser.add_argument(
        "tag",
        type=str,
        help="String to specify which version to retrieve from",
    )
    parser.add_argument(
        "-c",
        "--config",
        type=os.path.abspath,  # type: ignore
        required=False,
        default="config.cfg",
        help="Config file (see docs for further info)",
    )
    args = parser.parse_args()

    config = read_config(args.config, args.tag)
    config["bucket"] = args.bucket
    config["key"] = args.key
    config["buckettype"] = args.buckettype
    config["output"] = args.output

    retrieve_object(config)


def console_command() -> None:
    retcode = 1
    try:
        main()
        retcode = 0
    except Exception as err:  # pylint: disable=broad-exception-caught
        print(f"Error: {err}", file=sys.stderr)
    sys.exit(retcode)


if __name__ == "__main__":
    console_command()
