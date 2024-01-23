import os.path
from typing import Union

import botocore

from leveled_hotbackup_s3_sync import erlang
from leveled_hotbackup_s3_sync.utils import (
    download_bytes_from_s3,
    ensure_parent_dir_exists,
    is_s3_url,
    upload_bytes_to_s3,
)


def read_manifest(manifest_path: str, endpoint: Union[str, None] = None) -> list:
    if is_s3_url(manifest_path):
        try:
            manifest_data = download_bytes_from_s3(manifest_path, endpoint)
        except botocore.exceptions.ClientError as err:
            if err.response["Error"]["Code"] == "NoSuchKey":
                raise ValueError("Could not open journal manifest. Check provided TAG or s3_path.") from err
            raise err
    else:
        with open(manifest_path, "rb") as file_handle:
            manifest_data = file_handle.read()
    manifest = erlang.binary_to_term(manifest_data)
    return manifest


def save_local_manifest(new_manifest: list, filename: str) -> None:
    ensure_parent_dir_exists(filename)
    manifest = erlang.term_to_binary(new_manifest, compressed=True)
    print(f"Saving new manifest to {filename}")
    with open(filename, "wb") as file_handle:
        file_handle.write(manifest)


def upload_new_manifest(
    new_manifest: list, partition: str, destination: str, tag: str, endpoint: Union[str, None]
) -> str:
    manifest = erlang.term_to_binary(new_manifest, compressed=True)
    s3_path = os.path.join(destination, partition, f"journal/journal_manifest/{tag}.man")
    print(f"Uploading new manifest to {s3_path}")
    upload_bytes_to_s3(manifest, s3_path, endpoint)
    return s3_path
