from typing import Union

import cdblib


def create_hints_file(filename: str, journal_keys: list) -> None:
    with open(filename, "wb") as file_handle:
        with cdblib.Writer(file_handle) as writer:
            for k in journal_keys:
                sqn = k[0]
                if isinstance(k[2][1], tuple):
                    buckettype = k[2][1][0].value
                    bucket = k[2][1][1].value
                    bkey = k[2][2].value
                    writer.putint(buckettype + b"\x00" + bucket + b"\x00" + bkey, sqn)
                else:
                    bucket = k[2][1].value
                    bkey = k[2][2].value
                    writer.putint(bucket + b"\x00" + bkey, sqn)


def get_sqn(filename: str, bucket: bytes, bkey: bytes, buckettype: Union[bytes, None] = None) -> int:
    with cdblib.Reader.from_file_path(filename) as reader:
        if buckettype:
            return reader.getint(buckettype + b"\x00" + bucket + b"\x00" + bkey)
        return reader.getint(bucket + b"\x00" + bkey)
