import tempfile

import boto3
import botocore
import pytest
from moto import mock_s3

from leveled_hotbackup_s3_sync import erlang
from leveled_hotbackup_s3_sync.manifest import (
    read_manifest,
    save_local_manifest,
    upload_new_manifest,
)


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


MANIFEST_DATA = [
    (
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
    ),
    (
        486,
        b"/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/486_71e4b785-9c5d-4f3a-bb4a-d2e3fdc66945",
        erlang.OtpErlangPid(erlang.OtpErlangAtom(b"riak@172.17.0.2"), b"\x00\x00G\xe4", b"\x00\x00\x00\x01", b"\x02"),
        (
            971,
            erlang.OtpErlangAtom(b"stnd"),
            (
                erlang.OtpErlangAtom(b"o_rkv"),
                (erlang.OtpErlangBinary(b"testType"), erlang.OtpErlangBinary(b"typedBucket")),
                erlang.OtpErlangBinary(b"typedKey984"),
                erlang.OtpErlangAtom(b"null"),
            ),
        ),
    ),
    (
        0,
        b"/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/0_50f4666b-6ad8-4b6f-9e2a-23a235c82706",
        erlang.OtpErlangPid(
            erlang.OtpErlangAtom(b"riak@172.17.0.2"), b"\x00\x00\x03\xb0", b"\x00\x00\x00\x00", b"\x02"
        ),
        (
            485,
            erlang.OtpErlangAtom(b"stnd"),
            (
                erlang.OtpErlangAtom(b"o_rkv"),
                (erlang.OtpErlangBinary(b"testType"), erlang.OtpErlangBinary(b"typedBucket")),
                erlang.OtpErlangBinary(b"typedKey984"),
                erlang.OtpErlangAtom(b"null"),
            ),
        ),
    ),
]


def test_read_manifest(s3_client):
    manifest_filename = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_manifest/0.man"
    manifest = read_manifest(manifest_filename)

    assert len(manifest) == 3
    assert manifest[0][0] == 972
    assert manifest[1][0] == 486
    assert manifest[2][0] == 0
    assert (
        manifest[0][1]
        == b"/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a"
    )
    assert (
        manifest[1][1]
        == b"/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/486_71e4b785-9c5d-4f3a-bb4a-d2e3fdc66945"
    )
    assert (
        manifest[2][1]
        == b"/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/0_50f4666b-6ad8-4b6f-9e2a-23a235c82706"
    )

    s3_client.upload_file(manifest_filename, "test", "reads3manifest")

    manifest = read_manifest("s3://test/reads3manifest", None)

    assert len(manifest) == 3
    assert manifest[0][0] == 972
    assert manifest[1][0] == 486
    assert manifest[2][0] == 0
    assert (
        manifest[0][1]
        == b"/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a"
    )
    assert (
        manifest[1][1]
        == b"/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/486_71e4b785-9c5d-4f3a-bb4a-d2e3fdc66945"
    )
    assert (
        manifest[2][1]
        == b"/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/0_50f4666b-6ad8-4b6f-9e2a-23a235c82706"
    )

    with pytest.raises(ValueError) as err:
        read_manifest("s3://test/doesnotexist", None)
    assert str(err.value) == "Could not open journal manifest. Check provided TAG or s3_path."

    with pytest.raises(botocore.exceptions.ClientError) as err:
        read_manifest("s3://blah/blah", None)
    assert (
        str(err.value)
        == "An error occurred (NoSuchBucket) when calling the GetObject operation: The specified bucket does not exist"
    )


def test_save_local_manifest():
    with tempfile.NamedTemporaryFile() as file_handle:
        save_local_manifest(MANIFEST_DATA, file_handle.name)

        with open(file_handle.name, "rb") as reader:
            data = reader.read()

    assert (
        data == b"\x83P\x00\x00\x02\x9bx\x9c\xad\x91;N\x031\x10@\x9d\x9f\x84D\xc1I&\xeb\xb5\xc7c\xbbC4\x14\xb4\xf4\x8b"
        b'\x7f\xcb&\xbb\xf9(\xd9 \xe5Vp\x0e8\x06p\x0e\x1c"\x94&\x1di\xe6\xa3\xd1\x8c\xde\xd3t\x8c\xb1Q3\xf69~\xb4'
        b"l^\xf4\x8bu\xe1\x14/\xb54%`\x90\t\x90\x12\x81\x8f\\\x80D\x11\xd0\x1b\x8b^\xd9\x82\x17\xf3\xd5n\xb3t\xdd"
        b"_\xae\xeaY\x97\xb6\x85\xd5\xa2J$\xb8\n\x14@z\xe3\x01\xf9\xe1\x80K\t\x84U1\x04Mh\x8c{\x8e\xecf3s\xedm\xa9"
        b"\xc5\xb4\xd4S>\x15\x99\xe1\xe9\x803lF\x99g\xf2\x16\xd9x\xdb/c3\x8el\xb2\xaa6\xedK3\\\xe4\xf9U\x9f\xb6\xfd"
        b"\xe3~\x9d\x0e\xcdu\x9f\x8bx\xb7\x0bm\xeaO\xfdC\xda[\x83y\x7f\xb9\xeb\xba_\xbb\xc1\xf7E\xec\xd0P\xa5\xcb"
        b"\x84^\x1b\x056\xa8\x08XK\x07\xde\xa3\x83(\x92\xacc \xb2\xa8\xce\xda\xdd\x7ff\xc0\xc1\xd1n\xf4~1;\xc7Z"
        b"\xd6\xfc_\x8dW\x8a\xd7HD\x1e\xc8E\x03\xe8\xa9\x06\x9b\x84\x03!\x9d\x90*\x18\xa19\x9d\x7f\xdbkfcG\xb1\xc1"
        b"\xd7\x85\xc4\xe6?]{\xb6U"
    )


def test_save_local_manifest_encoding():
    manifest_filename = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/1324485858831130769622089379649131486563188867072/journal/journal_manifest/0.man"
    with open(manifest_filename, "rb") as file_handle:
        encoded_manifest_data = file_handle.read()
    decoded_manifest_data = erlang.binary_to_term(encoded_manifest_data)

    with tempfile.NamedTemporaryFile() as file_handle:
        save_local_manifest(decoded_manifest_data, file_handle.name)

        with open(file_handle.name, "rb") as reader:
            data = reader.read()

    assert data == encoded_manifest_data


def test_upload_new_manifest(s3_client):
    manifest_name = upload_new_manifest(MANIFEST_DATA, "0", "s3://test/upload_new_manifest", "123", None)

    s3_obj = s3_client.get_object(Bucket="test", Key="upload_new_manifest/0/journal/journal_manifest/123.man")

    assert manifest_name == "s3://test/upload_new_manifest/0/journal/journal_manifest/123.man"
    s3_data = s3_obj["Body"].read()
    assert (
        s3_data == b"\x83P\x00\x00\x02\x9bx\x9c\xad\x91;N\x031\x10@\x9d\x9f\x84D\xc1I&\xeb\xb5\xc7c\xbbC4\x14\xb4"
        b'\xf4\x8b\x7f\xcb&\xbb\xf9(\xd9 \xe5Vp\x0e8\x06p\x0e\x1c"\x94&\x1di\xe6\xa3\xd1\x8c\xde\xd3t\x8c\xb1Q3'
        b"\xf69~\xb4l^\xf4\x8bu\xe1\x14/\xb54%`\x90\t\x90\x12\x81\x8f\\\x80D\x11\xd0\x1b\x8b^\xd9\x82\x17\xf3\xd5"
        b"n\xb3t\xdd_\xae\xeaY\x97\xb6\x85\xd5\xa2J$\xb8\n\x14@z\xe3\x01\xf9\xe1\x80K\t\x84U1\x04Mh\x8c{\x8e\xecf3"
        b"s\xedm\xa9\xc5\xb4\xd4S>\x15\x99\xe1\xe9\x803lF\x99g\xf2\x16\xd9x\xdb/c3\x8el\xb2\xaa6\xedK3\\\xe4\xf9U"
        b"\x9f\xb6\xfd\xe3~\x9d\x0e\xcdu\x9f\x8bx\xb7\x0bm\xeaO\xfdC\xda[\x83y\x7f\xb9\xeb\xba_\xbb\xc1\xf7E\xec"
        b"\xd0P\xa5\xcb\x84^\x1b\x056\xa8\x08XK\x07\xde\xa3\x83(\x92\xacc \xb2\xa8\xce\xda\xdd\x7ff\xc0\xc1\xd1n"
        b"\xf4~1;\xc7Z\xd6\xfc_\x8dW\x8a\xd7HD\x1e\xc8E\x03\xe8\xa9\x06\x9b\x84\x03!\x9d\x90*\x18\xa19\x9d\x7f\xdb"
        b"kfcG\xb1\xc1\xd7\x85\xc4\xe6?]{\xb6U"
    )
