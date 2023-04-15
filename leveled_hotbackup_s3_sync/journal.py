import cdblib
import erlang

from leveled_hotbackup_s3_sync.hints import create_hints_file
from leveled_hotbackup_s3_sync.utils import (
    download_file_from_s3,
    ensure_parent_dir_exists,
    local_path_exists,
    s3_path_exists,
    swap_path,
    upload_file_to_s3,
)


def list_keys(filename: str) -> list:
    with cdblib.Reader.from_file_path(filename) as reader:
        return [erlang.binary_to_term(x) for x in reader.keys()]


def maybe_upload_journal(journal: tuple, source: str, destination: str, endpoint: str) -> None:
    journal_filename = f"{journal[1].decode('utf-8')}.cdb"
    hints_filename = f"{journal[1].decode('utf-8')}.hints.cdb"
    journal_s3_path = swap_path(journal_filename, source, destination)
    hints_s3_path = swap_path(hints_filename, source, destination)

    if s3_path_exists(journal_s3_path, endpoint):
        print(f"{journal_s3_path} already exists")
    else:
        journal_keys = list_keys(journal_filename)
        create_hints_file(hints_filename, journal_keys)

        print(f"Uploading {hints_filename} to {hints_s3_path}")
        upload_file_to_s3(hints_filename, hints_s3_path, endpoint)

        print(f"Uploading {journal_filename} to {journal_s3_path}")
        upload_file_to_s3(journal_filename, journal_s3_path, endpoint)


def maybe_download_journal(journal: tuple, source: str, destination: str, endpoint: str) -> None:
    journal_filename = f"{journal[1].decode('utf-8')}.cdb"
    journal_local_path = swap_path(journal_filename, source, destination)

    if local_path_exists(journal_local_path):
        print(f"{journal_local_path} already exists")
    else:
        print(f"Downloading {journal_filename} to {journal_local_path}")
        ensure_parent_dir_exists(journal_local_path)
        download_file_from_s3(journal_filename, journal_local_path, endpoint)


def update_journal_filename(journal: tuple, source: str, destination: str) -> tuple:
    current_journal_filename = journal[1].decode("utf-8")
    s3_path = swap_path(current_journal_filename, source, destination)
    return (journal[0], s3_path.encode("utf-8"), journal[2], journal[3])
