import os.path
from typing import Union

from leveled_hotbackup_s3_sync import erlang
from leveled_hotbackup_s3_sync.utils import (
    download_bytes_from_s3,
    ensure_parent_dir_exists,
    is_s3_url,
    list_s3_object_versions,
    upload_bytes_to_s3,
)


def read_manifest(manifest_path: str, version: Union[str, None] = None, endpoint: Union[str, None] = None) -> list:
    if is_s3_url(manifest_path):
        manifest_data = download_bytes_from_s3(manifest_path, endpoint, version=version)
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


def upload_new_manifest(new_manifest: list, partition: str, destination: str, endpoint: Union[str, None]) -> tuple:
    manifest = erlang.term_to_binary(new_manifest, compressed=True)
    s3_path = os.path.join(destination, partition, "journal/journal_manifest/0.man")
    print(f"Uploading new manifest to {s3_path}")
    version_id = upload_bytes_to_s3(manifest, s3_path, endpoint)
    return (s3_path, version_id)


def upload_manifests(s3_manifests: list, destination: str, endpoint: Union[str, None]) -> None:
    manifests_data = erlang.term_to_binary(s3_manifests)
    s3_path = get_manifests_path(destination)
    print(f"Uploading manifest list to {s3_path}")
    upload_bytes_to_s3(manifests_data, s3_path, endpoint)


def get_partition_manifest(
    source: str, partition: int, version: Union[str, None] = None, endpoint: Union[str, None] = None
):
    manifest_version, manifest_location = None, None
    if is_s3_url(source):
        manifests = get_manifests(source, version, endpoint)  # type:ignore
        for manifest in manifests:
            if os.path.join(source, str(partition), "journal/journal_manifest/0.man") == manifest[0].decode("utf-8"):
                manifest_location = manifest[0].decode("utf-8")
                manifest_version = manifest[1].decode("utf-8")
                break
        if not manifest_version:
            raise ValueError("Could not find partition ID in S3 MANIFESTS")
    else:
        manifest_location = os.path.join(source, str(partition), "journal/journal_manifest/0.man")
    return read_manifest(manifest_location, manifest_version, endpoint)  # type:ignore


def get_manifests_versions(destination: str, endpoint: Union[str, None]) -> list:
    s3_path = get_manifests_path(destination)
    return list_s3_object_versions(s3_path, endpoint)


def get_manifests(source: str, version: str, endpoint: Union[str, None]) -> list:
    s3_path = get_manifests_path(source)
    manifests_data = download_bytes_from_s3(s3_path, endpoint, version=version)
    return erlang.binary_to_term(manifests_data)


def get_manifests_path(parent_path: str) -> str:
    return os.path.join(parent_path, "MANIFESTS")


def guess_s3_ringsize(location: str, version: str, endpoint: Union[str, None]) -> int:
    manifests = get_manifests(location, version, endpoint)
    return len(manifests)
