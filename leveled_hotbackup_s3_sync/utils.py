import hashlib
import os
import os.path
from typing import Tuple, Union
from urllib.parse import urlparse

import boto3
from botocore.errorfactory import ClientError

from leveled_hotbackup_s3_sync import erlang

MAX_SHA_INT = 1461501637330902918203684832716283019655932542975


def str_to_bytes(convert_str: str) -> bytes:
    return convert_str.encode("utf-8")


def is_s3_url(url: str) -> bool:
    parsed_url = urlparse(url)
    if parsed_url.scheme == "s3":
        return True
    return False


def check_s3_url(url: str) -> str:
    if not is_s3_url(url):
        raise ValueError
    return url


def check_endpoint_url(url: str) -> str:
    parsed_url = urlparse(url)
    if parsed_url.path != "":
        raise ValueError
    return url


def parse_s3_url(path: str) -> Tuple[str, str]:
    parsed_url = urlparse(path)
    if parsed_url.scheme != "s3":
        raise ValueError(f"{path} is not a valid S3 URI")
    bucket = parsed_url.netloc
    key = parsed_url.path[1:]
    return bucket, key


def swap_path(filename: str, source: str, destination: str) -> str:
    return os.path.join(destination, os.path.relpath(filename, source))


def s3_path_exists(s3_path: str, endpoint: Union[str, None]) -> bool:
    s3_client = boto3.client("s3", endpoint_url=endpoint)
    bucket, key = parse_s3_url(s3_path)
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as err:
        if err.response["Error"]["Code"] == "404":
            return False
        raise err
    return True


def upload_file_to_s3(source: str, destination: str, endpoint: Union[str, None]) -> None:
    s3_client = boto3.client("s3", endpoint_url=endpoint)
    bucket, key = parse_s3_url(destination)
    s3_client.upload_file(source, bucket, key)


def upload_bytes_to_s3(data: bytes, destination: str, endpoint: Union[str, None]) -> None:
    s3_client = boto3.client("s3", endpoint_url=endpoint)
    bucket, key = parse_s3_url(destination)
    s3_client.put_object(Body=data, Bucket=bucket, Key=key)


def download_file_from_s3(s3_path: str, local_path: str, endpoint: Union[str, None]) -> None:
    s3_client = boto3.client("s3", endpoint_url=endpoint)
    bucket, key = parse_s3_url(s3_path)
    s3_client.download_file(bucket, key, local_path)


def download_bytes_from_s3(s3_path: str, endpoint: Union[str, None], version: Union[str, None] = None) -> bytes:
    s3_client = boto3.client("s3", endpoint_url=endpoint)
    bucket, key = parse_s3_url(s3_path)
    if version:
        response = s3_client.get_object(Bucket=bucket, Key=key, VersionId=version)
    else:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def local_path_exists(local_path: str) -> bool:
    return os.path.exists(local_path)


def ensure_parent_dir_exists(local_path: str) -> None:
    parent_dir = os.path.dirname(local_path)
    os.makedirs(parent_dir, exist_ok=True)


def riak_ring_indexes(ring_size: int) -> list:
    ring_increment = riak_ring_increment(ring_size)
    return [ring_increment * n + n for n in range(0, ring_size)]


def riak_ring_increment(ring_size: int) -> int:
    return MAX_SHA_INT // ring_size


def hash_bucket_key(bucket: bytes, bkey: bytes, buckettype: Union[bytes, None] = None) -> int:
    if buckettype:
        bucket_key = (
            (erlang.OtpErlangBinary(buckettype), erlang.OtpErlangBinary(bucket)),
            erlang.OtpErlangBinary(bkey),
        )
    else:
        bucket_key = (
            erlang.OtpErlangBinary(bucket),
            erlang.OtpErlangBinary(bkey),
        )  # type:ignore
    hashed_bucket_key = hashlib.sha1(erlang.term_to_binary(bucket_key)).digest()
    return int.from_bytes(hashed_bucket_key, byteorder="big")


def find_primary_partition(ring_size: int, bucket: bytes, bkey: bytes, buckettype: Union[bytes, None] = None) -> int:
    key_index = hash_bucket_key(bucket, bkey, buckettype)
    ring = riak_ring_indexes(ring_size)
    ring_increment = riak_ring_increment(ring_size)
    ring_position = (key_index // ring_increment + 1) % ring_size
    return ring[ring_position]


def create_journal_key(sqn: int, bucket: bytes, bkey: bytes, buckettype: Union[bytes, None] = None) -> bytes:
    if buckettype:
        typed_bucket = (
            erlang.OtpErlangBinary(buckettype),
            erlang.OtpErlangBinary(bucket),
        )
    else:
        typed_bucket = erlang.OtpErlangBinary(bucket)  # type: ignore
    journal_key = (
        sqn,
        erlang.OtpErlangAtom(b"stnd"),
        (
            erlang.OtpErlangAtom(b"o_rkv"),
            typed_bucket,
            erlang.OtpErlangBinary(bkey),
            erlang.OtpErlangAtom(b"null"),
        ),
    )
    return erlang.term_to_binary(journal_key)


def find_latest_ring(ring_directory: str) -> str:
    filename = ""
    with os.scandir(ring_directory) as itr:
        for file in itr:
            if file.name.startswith("riak_core_ring.") and file.name > filename:
                filename = file.name
    if filename == "":
        raise ValueError(f"{ring_directory} is not a valid Riak Ring location")
    return os.path.join(ring_directory, filename)


def get_ring_size(ring_filename: str) -> int:
    with open(ring_filename, "rb") as file_handle:
        ring_data = erlang.binary_to_term(file_handle.read())
    return ring_data[3][0]


def get_owned_partitions(ring_filename: str) -> list:
    owned_partitions = []
    with open(ring_filename, "rb") as file_handle:
        ring_data = erlang.binary_to_term(file_handle.read())
    this_node = ring_data[1]
    for partition in ring_data[3][1]:
        if partition[1] == this_node:
            owned_partitions.append(partition[0])
    return owned_partitions


# pylint: disable=too-few-public-methods
class RiakObject:
    def __init__(self):
        self.vector_clocks = []
        self.siblings = []

    def _decode_maybe_binary(self, data: bytes):
        is_binary = int.from_bytes(data[:1], "big")
        if is_binary:
            return data[1:]
        return erlang.binary_to_term(data[1:])

    def _decode_metadata(self, metadata_bin: bytes) -> dict:
        metadata = {}
        offset = 0
        lm_mega = int.from_bytes(metadata_bin[offset : offset + 4], "big")
        offset += 4
        lm_secs = int.from_bytes(metadata_bin[offset : offset + 4], "big")
        offset += 4
        lm_micro = int.from_bytes(metadata_bin[offset : offset + 4], "big")
        offset += 4
        metadata["last_modified"] = str(lm_mega) + str(lm_secs).zfill(6) + "." + str(lm_micro).zfill(6)

        vtag_len = int.from_bytes(metadata_bin[offset : offset + 1], "big")
        offset += 1
        metadata["vtag"] = metadata_bin[offset : offset + vtag_len]  # type:ignore
        offset += vtag_len

        metadata["deleted"] = int.from_bytes(metadata_bin[offset : offset + 1], "big")  # type:ignore
        offset += 1

        while offset < len(metadata_bin):
            key_len = int.from_bytes(metadata_bin[offset : offset + 4], "big")
            offset += 4
            key = self._decode_maybe_binary(metadata_bin[offset : offset + key_len]).decode("utf-8")
            offset += key_len

            val_len = int.from_bytes(metadata_bin[offset : offset + 4], "big")
            offset += 4
            val = self._decode_maybe_binary(metadata_bin[offset : offset + val_len])
            offset += val_len

            metadata[key] = val

        return metadata

    def decode(self, obj_bin: bytes) -> None:
        offset = 0
        magic_number = int.from_bytes(obj_bin[offset : offset + 1], "big")
        offset += 1
        if magic_number != 53:
            raise ValueError("Decode error, wrong magic number")
        object_version = int.from_bytes(obj_bin[offset : offset + 1], "big")
        offset += 1
        if object_version != 1:
            raise ValueError("Decode error, wrong object version")
        vector_clocks_len = int.from_bytes(obj_bin[offset : offset + 4], "big")
        offset += 4
        self.vector_clocks = erlang.binary_to_term(obj_bin[offset : offset + vector_clocks_len])
        offset += vector_clocks_len
        siblings_count = int.from_bytes(obj_bin[offset : offset + 4], "big")
        offset += 4
        for _ in range(siblings_count):
            value_len = int.from_bytes(obj_bin[offset : offset + 4], "big")
            offset += 4
            value = self._decode_maybe_binary(obj_bin[offset : offset + value_len])
            offset += value_len
            metadata_len = int.from_bytes(obj_bin[offset : offset + 4], "big")
            offset += 4
            metadata = self._decode_metadata(obj_bin[offset : offset + metadata_len])
            offset += metadata_len
            self.siblings.append({"value": value, "metadata": metadata})
        if offset != len(obj_bin):
            raise ValueError("Decode error, unexpected data")
