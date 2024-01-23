import argparse
import os
import os.path
import sys

from leveled_hotbackup_s3_sync.config import read_config
from leveled_hotbackup_s3_sync.journal import (
    maybe_download_journal,
    maybe_upload_journal,
    update_journal_filename,
)
from leveled_hotbackup_s3_sync.manifest import (
    read_manifest,
    save_local_manifest,
    upload_new_manifest,
)
from leveled_hotbackup_s3_sync.utils import get_owned_partitions


def backup(config: dict) -> None:
    partitions = get_owned_partitions(config["ring_filename"])
    for partition in partitions:
        manifest_filename = os.path.join(config["hotbackup_path"], str(partition), "journal/journal_manifest/0.man")
        print(f"Starting to process {manifest_filename}")
        manifest = read_manifest(manifest_filename)

        new_manifest = []
        for journal in manifest:
            maybe_upload_journal(
                journal, config["hotbackup_path"], config["s3_path"], config["hints_files"], config["s3_endpoint"]
            )
            new_manifest.append(update_journal_filename(journal, config["hotbackup_path"], config["s3_path"]))

        upload_new_manifest(new_manifest, str(partition), config["s3_path"], config["tag"], config["s3_endpoint"])


def restore(config: dict) -> None:
    partitions = get_owned_partitions(config["ring_filename"])
    for partition in partitions:
        manifest_s3_path = os.path.join(
            config["s3_path"], str(partition), f"journal/journal_manifest/{config['tag']}.man"
        )
        print(f"Starting to process {manifest_s3_path}")
        manifest = read_manifest(manifest_s3_path, config["s3_endpoint"])

        new_manifest = []
        for journal in manifest:
            maybe_download_journal(journal, config["s3_path"], config["leveled_path"], config["s3_endpoint"])
            new_manifest.append(update_journal_filename(journal, config["s3_path"], config["leveled_path"]))
        manifest_filename = os.path.join(config["leveled_path"], str(partition), "journal/journal_manifest/0.man")
        save_local_manifest(new_manifest, manifest_filename)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="Riak Backup Sync",
        description="Synchronise Riak hot-backup between S3 and local",
    )
    parser.add_argument(
        "action",
        choices=["backup", "restore"],
        help="Specify operation to perform",
    )
    parser.add_argument(
        "tag",
        type=str,
        help="String to tag a backup, or to specify which version to restore from",
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

    if args.action == "backup":
        backup(config)

    if args.action == "restore":
        restore(config)


def console_command() -> None:
    retcode = 1
    try:
        main()
        retcode = 0
    except Exception as err:  # pylint: disable=broad-exception-caught
        print(f"Error: {err}", file=sys.stderr)
    sys.exit(retcode)
