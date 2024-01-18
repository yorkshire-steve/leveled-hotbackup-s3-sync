import os.path
import tempfile

import boto3
import pytest
from moto import mock_s3

from leveled_hotbackup_s3_sync import erlang
from leveled_hotbackup_s3_sync.journal import (
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
        s3_client.put_bucket_versioning(
            Bucket="test",
            VersioningConfiguration={
                "MFADelete": "Disabled",
                "Status": "Enabled",
            },
        )
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
    assert s3_head["VersionId"] == s3_obj["VersionId"]

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

    s3_obj = s3_client.get_object(Bucket="test", Key=f"maybe_upload_journal_hints/{short_filename}.hints.cdb")
    with open(
        f"{HOTBACKUP_DIR}/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a.hints.cdb",
        "rb",
    ) as file_handle:
        assert file_handle.read() == s3_obj["Body"].read()


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
