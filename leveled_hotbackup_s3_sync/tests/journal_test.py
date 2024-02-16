import os.path
import tempfile

import boto3
import pytest
from moto import mock_s3

from leveled_hotbackup_s3_sync import erlang
from leveled_hotbackup_s3_sync.journal import (
    decode_journal_object,
    list_keys,
    maybe_download_journal,
    maybe_upload_journal,
    update_journal_filename,
)

HOTBACKUP_DIR = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59"


@pytest.fixture(name="s3_client")
def fixture_s3_client():
    with mock_s3():
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket="test")
        yield s3_client


def test_list_keys():
    filename = f"{HOTBACKUP_DIR}/0/journal/journal_files/0_50f4666b-6ad8-4b6f-9e2a-23a235c82706.cdb"
    keys = list_keys(filename)

    assert len(keys) == 485
    assert keys[0] == (
        1,
        erlang.OtpErlangAtom(b"stnd"),
        (
            erlang.OtpErlangAtom(b"o_rkv"),
            erlang.OtpErlangBinary(b"testBucket"),
            erlang.OtpErlangBinary(b"testKey17"),
            erlang.OtpErlangAtom(b"null"),
        ),
    )
    assert keys[384] == (
        385,
        erlang.OtpErlangAtom(b"stnd"),
        (
            erlang.OtpErlangAtom(b"o_rkv"),
            erlang.OtpErlangBinary(b"testBucket"),
            erlang.OtpErlangBinary(b"testKey8636"),
            erlang.OtpErlangAtom(b"null"),
        ),
    )


def test_decode_journal_object():
    test_key = (
        b"\x83h\x03b\x00\x00\x01\xfad\x00\x04stndh\x04d\x00\x05o_rkvm\x00\x00\x00\ntestBucketm\x00\x00\x00\ntestKey"
        b"992d\x00\x04null"
    )
    test_obj = bytearray(
        b"f\xe7J\xa7x\x9c3ed``Pk\xce\x01\x92\x8c\x19L\xb9@\x8ag?\xc3\xc2\xf7\xf3\x8e\\\xf9\x07d\x8bd0%2\xe5\xb12\xfc"
        b"\xdf0\xfd._\x16H\r\x10\x8b3V+\x95\xa4\x16\x97(Y)\x15\xa5\x16\xe4$&\xa7\xa6XZ\x1a)\xd5\x02\xa5\xda\x19\x18"
        b'\xd8&00\x14\xfcg`\xbf\xfaW\xcc\xac\xd0-\xd8\xdf\xc9\xc5,\xaa"$"\xdc\xb9*\xaf<\xd39\xc5\xd7\x82\x01d\x07c'
        b'\x84nPfb\xb6\xaeojI"\x90\xcf\xcc\xd0\x0c2\x9d\x8d13/%\xb5\x02!\xc0\xcb\x98\x9c\x9fW\x92\x9aW\xa2[RY\x90\n'
        b"\x14\x10eh\xcef\x10H,(\xc8\xc9LN,\xc9\xcc\xcf\xd3\xcf*\xce\xcf\x03k\xf5\xc9\xcc\xcb.\x86j\x05\x00Rz77\x83"
        b"h\x02jd\x00\x08infinity\x00\x00\x00\x0f\x03"
    )
    assert (
        decode_journal_object(test_key, test_obj)
        == b"5\x01\x00\x00\x00&\x83l\x00\x00\x00\x01h\x02m\x00\x00\x00\x0c\xbf\x00\xa1\xef\x9e\xc4\xd4\xfe\x00\x00"
        b'\x00\x14h\x02a\x02n\x05\x00\xff\xb0\x97\xdd\x0ej\x00\x00\x00\x01\x00\x00\x00\x17\x01{"test":"replaced992"}'
        b"\x00\x00\x00\x87\x00\x00\x06\x90\x00\x00p\xff\x00\x07\xd5\xfd\x166qFSOBD6ZxTXWCznwiCdM8\x00\x00\x00\x00\x0c"
        b"\x01X-Riak-Meta\x00\x00\x00\x03\x00\x83j\x00\x00\x00\x06\x01index\x00\x00\x00\x03\x00\x83j\x00\x00\x00\r\x01"
        b"content-type\x00\x00\x00\x15\x00\x83k\x00\x10application/json\x00\x00\x00\x06\x01Links\x00\x00\x00\x03\x00"
        b"\x83j"
    )

    test_obj[1] = 0
    with pytest.raises(ValueError) as err:
        decode_journal_object(test_key, test_obj)
    assert str(err.value) == "CRC error retrieving object"

    test_obj2 = b"\x8c\xe3\xff \x03\x00\x00\x000abc\x00\x00\x00\x00\x07"
    assert decode_journal_object(b"", test_obj2) == b"abc"

    test_obj3 = b"\xc0\xd9ae\x83l\x00\x00\x00\x03a\x00a\x01a\x02j\x00\x00\x00\x00\x00"
    assert decode_journal_object(b"", test_obj3) == [0, 1, 2]

    test_obj4 = b"\xcc\x1d.\x9fx\x9ck\xcea```NdIdMd\xcb\x02\x00\x12-\x02\x8f\x00\x00\x00\x00\x01"
    assert decode_journal_object(b"", test_obj4) == [4, 5, 6]


def test_maybe_upload_journal(s3_client):
    journal = (
        972,
        b"/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a",
        erlang.OtpErlangPid(erlang.OtpErlangAtom(b"riak@172.17.0.2"), b"\x00\x00\x03`", b"\x00\x00\x00\x03", b"\x02"),
        (
            1457,
            erlang.OtpErlangAtom(b"stnd"),
            (
                erlang.OtpErlangAtom(b"o_rkv"),
                (erlang.OtpErlangBinary(b"testType"), erlang.OtpErlangBinary(b"typedBucket")),
                erlang.OtpErlangBinary(b"typedKey984"),
                erlang.OtpErlangAtom(b"null"),
            ),
        ),
    )
    maybe_upload_journal(journal, f"{HOTBACKUP_DIR}/", "s3://test/maybe_upload_journal/", False, None)

    s3_obj = s3_client.get_object(
        Bucket="test",
        Key="maybe_upload_journal/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a.cdb",
    )

    with open(
        f"{HOTBACKUP_DIR}/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a.cdb",
        "rb",
    ) as file_handle:
        assert file_handle.read() == s3_obj["Body"].read()

    # Attempt upload again to test only upload if not exists
    maybe_upload_journal(journal, f"{HOTBACKUP_DIR}/", "s3://test/maybe_upload_journal/", False, None)

    s3_head = s3_client.head_object(
        Bucket="test",
        Key="maybe_upload_journal/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a.cdb",
    )
    assert s3_head["LastModified"] == s3_obj["LastModified"]

    # Now test hints file creation
    with tempfile.NamedTemporaryFile(suffix=".cdb") as file_handle:
        with open(
            f"{HOTBACKUP_DIR}/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a.cdb",
            "rb",
        ) as reader:
            file_handle.write(reader.read())
        file_handle.flush()
        filename = file_handle.name
        maybe_upload_journal(
            (0, filename[:-4].encode("utf-8")),
            os.path.dirname(filename),
            "s3://test/maybe_upload_journal_hints",
            True,
            None,
        )

    short_filename = os.path.basename(filename)[:-4]
    s3_obj = s3_client.get_object(Bucket="test", Key=f"maybe_upload_journal_hints/{short_filename}.cdb")
    with open(
        f"{HOTBACKUP_DIR}/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a.cdb",
        "rb",
    ) as file_handle:
        assert file_handle.read() == s3_obj["Body"].read()

    s3_head = s3_client.head_object(Bucket="test", Key=f"maybe_upload_journal_hints/{short_filename}.hints.cdb")
    assert s3_head["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_maybe_download_journal(s3_client):
    s3_client.put_object(Bucket="test", Key="maybe_download_journal/test.cdb", Body=b"testbody")

    with tempfile.TemporaryDirectory() as tmpdir:
        maybe_download_journal(
            (0, b"s3://test/maybe_download_journal/test"), "s3://test/maybe_download_journal", tmpdir, None
        )
        with open(f"{tmpdir}/test.cdb", "rb") as file_handle:
            assert file_handle.read() == b"testbody"

        # Now change contents of the file, try and download again, it shouldn't be overwritten
        with open(f"{tmpdir}/test.cdb", "wb") as file_handle:
            file_handle.write(b"newbody")

        maybe_download_journal(
            (0, b"s3://test/maybe_download_journal/test"), "s3://test/maybe_download_journal", tmpdir, None
        )
        with open(f"{tmpdir}/test.cdb", "rb") as file_handle:
            assert file_handle.read() == b"newbody"


def test_update_journal_filename():
    journal = (
        972,
        b"/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a",
        erlang.OtpErlangPid(erlang.OtpErlangAtom(b"riak@172.17.0.2"), b"\x00\x00\x03`", b"\x00\x00\x00\x03", b"\x02"),
        (
            1457,
            erlang.OtpErlangAtom(b"stnd"),
            (
                erlang.OtpErlangAtom(b"o_rkv"),
                (erlang.OtpErlangBinary(b"testType"), erlang.OtpErlangBinary(b"typedBucket")),
                erlang.OtpErlangBinary(b"typedKey984"),
                erlang.OtpErlangAtom(b"null"),
            ),
        ),
    )
    new_journal = update_journal_filename(journal, HOTBACKUP_DIR, "/new/path")

    assert new_journal == (
        journal[0],
        b"/new/path/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a",
        journal[2],
        journal[3],
    )
