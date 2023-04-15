import os.path

import erlang

from leveled_hotbackup_s3_sync.utils import (
    download_bytes_from_s3,
    ensure_parent_dir_exists,
    list_s3_object_versions,
    upload_bytes_to_s3,
)


def read_manifest(filename: str) -> list:
    with open(filename, "rb") as file_handle:
        manifest = erlang.binary_to_term(file_handle.read())
    return manifest


def read_s3_manifest(s3_path: str, version: str, endpoint: str) -> list:
    manifest_data = download_bytes_from_s3(s3_path, endpoint, version=version)
    manifest = erlang.binary_to_term(manifest_data)
    return manifest


def save_local_manifest(new_manifest: list, filename: str) -> None:
    ensure_parent_dir_exists(filename)
    manifest = erlang.term_to_binary(new_manifest)
    print(f"Saving new manifest to {filename}")
    with open(filename, "wb") as file_handle:
        file_handle.write(manifest)


def upload_new_manifest(new_manifest: list, partition: str, destination: str, endpoint: str) -> str:
    manifest = erlang.term_to_binary(new_manifest)
    s3_path = os.path.join(destination, partition, "journal/journal_manifest/0.man")
    print(f"Uploading new manifest to {s3_path}")
    version_id = upload_bytes_to_s3(manifest, s3_path, endpoint)
    return f"{s3_path}#{version_id}"


def upload_manifests(s3_manifests: list, destination: str, endpoint: str) -> None:
    manifests_data = "\n".join(s3_manifests).encode("utf-8")
    s3_path = get_manifests_path(destination)
    print(f"Uploading manifest list to {s3_path}")
    upload_bytes_to_s3(manifests_data, s3_path, endpoint)


def get_manifests_versions(destination: str, endpoint: str) -> list:
    s3_path = get_manifests_path(destination)
    return list_s3_object_versions(s3_path, endpoint)


def get_manifests(source: str, version: str, endpoint: str) -> list:
    s3_path = get_manifests_path(source)
    manifests_data = download_bytes_from_s3(s3_path, endpoint, version=version)
    manifests = []
    for line in manifests_data.decode("utf-8").split("\n"):
        linedata = line.split("#")
        manifests.append((linedata[0], linedata[1]))
    return manifests


def get_manifests_path(parent_path: str) -> str:
    return os.path.join(parent_path, "MANIFESTS")
