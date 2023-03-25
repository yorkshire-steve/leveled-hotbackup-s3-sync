import sys

import cdblib

def create_hints_file(filename: str, journal_keys: list) -> None:
    with open(filename, "wb") as f:
        with cdblib.Writer(f) as writer:
            for k in journal_keys:
                sqn = k[0]
                bucket = k[2][1].value
                bkey = k[2][2].value
                writer.putint(bucket + b"\x00" + bkey, sqn)

def get_sqn(filename: str, bucket: bytes, bkey: bytes) -> int:
    with cdblib.Reader.from_file_path(filename) as reader:
        return reader.getint(bucket + b"\x00" + bkey)

if __name__ == "__main__":
    filename = sys.argv[1]
    bucket = sys.argv[2].encode('utf-8')
    bkey = sys.argv[3].encode('utf-8')
    sqn = get_sqn(filename, bucket, bkey)
    print(sqn)
