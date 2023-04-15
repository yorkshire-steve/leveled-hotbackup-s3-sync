import argparse
import os
import os.path
from urllib.parse import urlparse

from leveled_hotbackup_s3_sync.journal import (
    maybe_download_journal,
    maybe_upload_journal,
    update_journal_filename,
)
from leveled_hotbackup_s3_sync.manifest import (
    get_manifests,
    get_manifests_versions,
    read_manifest,
    read_s3_manifest,
    save_local_manifest,
    upload_manifests,
    upload_new_manifest,
)
from leveled_hotbackup_s3_sync.utils import swap_path


def backup(source: str, destination: str, endpoint: str) -> None:
    s3_manifests = []
    partitions = os.listdir(source)
    for partition in partitions:
        manifest_filename = os.path.join(source, partition, "journal/journal_manifest/0.man")
        print(f"Starting to process {manifest_filename}")
        manifest = read_manifest(manifest_filename)

        new_manifest = []
        for journal in manifest:
            maybe_upload_journal(journal, source, destination, endpoint)
            new_manifest.append(update_journal_filename(journal, source, destination))

        s3_path = upload_new_manifest(new_manifest, partition, destination, endpoint)
        s3_manifests.append(s3_path)
    upload_manifests(s3_manifests, destination, endpoint)


def restore(source: str, version: str, destination: str, endpoint: str) -> None:
    manifests_list = get_manifests(source, version, endpoint)
    for manifest_path_version in manifests_list:
        print(f"Starting to process {manifest_path_version[0]}")
        manifest = read_s3_manifest(manifest_path_version[0], manifest_path_version[1], endpoint)

        new_manifest = []
        for journal in manifest:
            maybe_download_journal(journal, source, destination, endpoint)
            new_manifest.append(update_journal_filename(journal, source, destination))
        manifest_filename = swap_path(manifest_path_version[0], source, destination)
        save_local_manifest(new_manifest, manifest_filename)


def list_versions(destination: str, endpoint: str) -> None:
    manifest_versions = get_manifests_versions(destination, endpoint)
    for manifest in manifest_versions:
        print(manifest["LastModified"], manifest["VersionId"])


def check_s3_url(url: str) -> str:
    parsed_url = urlparse(url)
    if parsed_url.scheme != "s3":
        raise ValueError
    return url


def check_endpoint_url(url: str) -> str:
    parsed_url = urlparse(url)
    if parsed_url.path != "":
        raise ValueError
    return url


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="Riak Backup Sync",
        description="Synchronise Riak hot-backup between S3 and local",
    )
    parser.add_argument(
        "-a",
        "--action",
        choices=["backup", "restore", "list"],
        default="backup",
        required=False,
    )
    parser.add_argument(
        "-l",
        "--local",
        type=os.path.abspath,  # type:ignore
        required=False,
        help="Local directory",
    )
    parser.add_argument("-s", "--s3", type=check_s3_url, required=True, help="S3 path")
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
        help="VersionId of MANIFESTS to restore from",
    )
    args = parser.parse_args()

    if args.action == "backup":
        if not args.local:
            raise ValueError("Must specify local directory to backup from")
        backup(args.local, args.s3, args.endpoint)

    if args.action == "list":
        list_versions(args.s3, args.endpoint)

    if args.action == "restore":
        if not args.local:
            raise ValueError("Must specify local directory to restore to")
        if not args.version:
            raise ValueError("Must specify VersionId of MANIFESTS file to restore from")
        restore(args.s3, args.version, args.local, args.endpoint)
