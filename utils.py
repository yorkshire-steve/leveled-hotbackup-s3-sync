import hashlib
import os
import os.path
from urllib.parse import urlparse

import boto3
from botocore.errorfactory import ClientError
import erlang

MAX_SHA_INT = 1461501637330902918203684832716283019655932542975

def parse_s3_url(path: str) -> dict:
    parsed_url = urlparse(path)
    if parsed_url.scheme != "s3":
        raise ValueError
    bucket = parsed_url.netloc
    key = parsed_url.path[1:]
    return bucket, key

def swap_path(filename: str, source: str, destination: str) -> str:
    return os.path.join(destination, os.path.relpath(filename, source))

def s3_path_exists(s3_path: str, endpoint: str) -> bool:
    s3 = boto3.client('s3', endpoint_url=endpoint)
    bucket, key = parse_s3_url(s3_path)
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise e
    return True

def upload_file_to_s3(source: str, destination: str, endpoint: str) -> None:
    s3 = boto3.client('s3', endpoint_url=endpoint)
    bucket, key = parse_s3_url(destination)
    s3.upload_file(source, bucket, key)

def upload_bytes_to_s3(data: bytes, destination: str, endpoint: str) -> str:
    s3 = boto3.client('s3', endpoint_url=endpoint)
    bucket, key = parse_s3_url(destination)
    response = s3.put_object(Body=data, Bucket=bucket, Key=key)
    return response['VersionId']

def list_s3_object_versions(s3_path: str, endpoint: str) -> list:
    s3 = boto3.client('s3', endpoint_url=endpoint)
    bucket, key = parse_s3_url(s3_path)
    response = s3.list_object_versions(Bucket=bucket, Prefix=key)
    return response['Versions']

def download_file_from_s3(s3_path: str, local_path: str, endpoint: str) -> None:
    s3 = boto3.client('s3', endpoint_url=endpoint)
    bucket, key = parse_s3_url(s3_path)
    s3.download_file(bucket, key, local_path)

def download_bytes_from_s3(s3_path: str, endpoint: str, version: str = None) -> bytes:
    s3 = boto3.client('s3', endpoint_url=endpoint)
    bucket, key = parse_s3_url(s3_path)
    if version:
        response = s3.get_object(Bucket=bucket, Key=key, VersionId=version)
    else:
        response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read()

def local_path_exists(local_path: str) -> bool:
    return os.path.exists(local_path)

def ensure_parent_dir_exists(local_path: str) -> None:
    parent_dir = os.path.dirname(local_path)
    os.makedirs(parent_dir, exist_ok=True)

def riak_ring_indexes(ring_size: int) -> list:
    ring_increment = riak_ring_increment(ring_size)
    return [ring_increment*n+n for n in range(0,ring_size)]

def riak_ring_increment(ring_size: int) -> int:
    return MAX_SHA_INT // ring_size

def hash_bucket_key(bucket: bytes, bkey: bytes, buckettype: bytes = None) -> int:
    if buckettype:
        bk = ((erlang.OtpErlangBinary(buckettype,bits=8), erlang.OtpErlangBinary(bucket,bits=8)), erlang.OtpErlangBinary(bkey,bits=8))
    else:
        bk = (erlang.OtpErlangBinary(bucket,bits=8), erlang.OtpErlangBinary(bkey,bits=8))
    hashed_bk = hashlib.sha1(erlang.term_to_binary(bk)).digest()
    return int.from_bytes(hashed_bk, byteorder="big")

def find_primary_partition(ring_size: int, bucket: bytes, bkey: bytes, buckettype: bytes = None) -> int:
    key_index = hash_bucket_key(bucket, bkey, buckettype)
    ring = riak_ring_indexes(ring_size)
    ring_increment = riak_ring_increment(ring_size)
    ring_position = (key_index // ring_increment + 1) % ring_size
    return ring[ring_position]
