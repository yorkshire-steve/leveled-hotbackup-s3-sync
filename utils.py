import os
import os.path
from urllib.parse import urlparse

import boto3
from botocore.errorfactory import ClientError

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
