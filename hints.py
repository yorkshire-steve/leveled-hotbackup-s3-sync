import sys

import cdblib

def create_hints_file(filename: str, journal_keys: list) -> None:
    with open(filename, "wb") as f:
        with cdblib.Writer(f) as writer:
            for k in journal_keys:
                sqn = k[0]
                if type(k[2][1]) == tuple:
                    buckettype = k[2][1][0].value
                    bucket = k[2][1][1].value
                    bkey = k[2][2].value
                    writer.putint(buckettype + b"\x00" + bucket + b"\x00" + bkey, sqn)
                else:
                    bucket = k[2][1].value
                    bkey = k[2][2].value
                    writer.putint(bucket + b"\x00" + bkey, sqn)

def get_sqn(filename: str, bucket: bytes, bkey: bytes, buckettype: bytes = None) -> int:
    with cdblib.Reader.from_file_path(filename) as reader:
        if buckettype:
            return reader.getint(buckettype + b"\x00" + bucket + b"\x00" + bkey)
        else:
            return reader.getint(bucket + b"\x00" + bkey)

if __name__ == "__main__":
    filename = sys.argv[1]
    bucket = sys.argv[2].encode('utf-8')
    bkey = sys.argv[3].encode('utf-8')
    btype = None
    if len(sys.argv) == 5:
        btype = sys.argv[4].encode('utf-8')
    sqn = get_sqn(filename, bucket, bkey, btype)
    print(sqn)
