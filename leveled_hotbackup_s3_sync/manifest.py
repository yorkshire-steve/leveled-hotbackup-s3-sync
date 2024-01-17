import os.path
from typing import Union

from leveled_hotbackup_s3_sync import erlang
from leveled_hotbackup_s3_sync.utils import (
    download_bytes_from_s3,
    ensure_parent_dir_exists,
    upload_bytes_to_s3,
)


def read_manifest(filename: str) -> list:
    with open(filename, "rb") as file_handle:
        manifest = erlang.binary_to_term(file_handle.read())
    return manifest


def read_s3_manifest(s3_path: str, endpoint: Union[str, None]) -> list:
    manifest_data = download_bytes_from_s3(s3_path, endpoint)
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
) -> tuple:
    manifest = erlang.term_to_binary(new_manifest, compressed=True)
    s3_path = os.path.join(destination, partition, f"journal/journal_manifest/{tag}.man")
    print(f"Uploading new manifest to {s3_path}")
    version_id = upload_bytes_to_s3(manifest, s3_path, endpoint)
    return (s3_path, version_id)
