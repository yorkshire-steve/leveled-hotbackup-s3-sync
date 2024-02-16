import os
import zlib
from typing import Union

import cdblib
import lz4.block

from leveled_hotbackup_s3_sync import erlang
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


def decode_journal_object(journal_key: bytes, journal_obj: bytes):
    journal_binary_len = len(journal_obj) - 5
    key_change_length = int.from_bytes(journal_obj[journal_binary_len : journal_binary_len + 4], "big")
    value_type = int.from_bytes(journal_obj[-1:], "big")
    is_binary, is_compressed, is_lz4 = decode_valuetype(value_type)

    crc = int.from_bytes(journal_obj[:4], "big")
    calc_crc = zlib.crc32(journal_key + journal_obj[4:])
    if crc != calc_crc:
        raise ValueError("CRC error retrieving object")

    journal_binary = journal_obj[4 : journal_binary_len - key_change_length]
    if is_compressed:
        if is_lz4:
            journal_binary = lz4.block.decompress(journal_binary)
        else:
            journal_binary = zlib.decompress(journal_binary)
    if is_binary:
        return journal_binary
    return erlang.binary_to_term(journal_binary)


def decode_valuetype(value_type: int) -> tuple:
    is_compressed = value_type & 1 == 1
    is_binary = value_type & 2 == 2
    is_lz4 = value_type & 4 == 4
    return is_binary, is_compressed, is_lz4


def maybe_upload_journal(
    journal: tuple, source: str, destination: str, create_hints_files: bool, endpoint: Union[str, None]
) -> None:
    journal_filename = f"{journal[1].decode('utf-8')}.cdb"
    journal_s3_path = swap_path(journal_filename, source, destination)

    if s3_path_exists(journal_s3_path, endpoint):
        print(f"{journal_s3_path} already exists")
    else:
        if create_hints_files:
            hints_filename = f"{journal[1].decode('utf-8')}.hints.cdb"
            hints_s3_path = swap_path(hints_filename, source, destination)

            journal_keys = list_keys(journal_filename)
            create_hints_file(hints_filename, journal_keys)

            print(f"Uploading {hints_filename} to {hints_s3_path}")
            upload_file_to_s3(hints_filename, hints_s3_path, endpoint)

            print(f"Deleting local copy of {hints_filename}")
            os.remove(hints_filename)

        print(f"Uploading {journal_filename} to {journal_s3_path}")
        upload_file_to_s3(journal_filename, journal_s3_path, endpoint)


def maybe_download_journal(journal: tuple, source: str, destination: str, endpoint: Union[str, None]) -> None:
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
