from typing import Union

import cdblib

from leveled_hotbackup_s3_sync import erlang


def create_hints_file(filename: str, journal_keys: list) -> None:
    with open(filename, "wb") as file_handle:
        with cdblib.Writer(file_handle) as writer:
            for k in journal_keys:
                sqn = k[0]
                bucket = k[2][1]
                bkey = k[2][2]
                cdb_key = erlang.term_to_binary((bucket, bkey))
                writer.putint(cdb_key, sqn)


def get_sqn(reader: cdblib.Reader, bucket: bytes, bkey: bytes, buckettype: Union[bytes, None] = None) -> int:
    if buckettype:
        cdb_key = erlang.term_to_binary(
            ((erlang.OtpErlangBinary(buckettype), erlang.OtpErlangBinary(bucket)), erlang.OtpErlangBinary(bkey))
        )
    else:
        cdb_key = erlang.term_to_binary((erlang.OtpErlangBinary(bucket), erlang.OtpErlangBinary(bkey)))
    return reader.getint(cdb_key)
