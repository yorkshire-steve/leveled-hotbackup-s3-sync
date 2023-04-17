import os.path
import tempfile

import boto3
import erlang
import pytest
from moto import mock_s3

from leveled_hotbackup_s3_sync.manifest import (
    get_manifests,
    get_manifests_path,
    get_manifests_versions,
    read_manifest,
    read_s3_manifest,
    save_local_manifest,
    upload_manifests,
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


def test_read_manifest():
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


def test_read_s3_manifest(s3_client):
    manifest_filename = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_manifest/0.man"
    s3_client.upload_file(manifest_filename, "test", "reads3manifest")
    response = s3_client.head_object(Bucket="test", Key="reads3manifest")

    manifest = read_s3_manifest("s3://test/reads3manifest", response["VersionId"], None)

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


def test_save_local_manifest():
    with tempfile.NamedTemporaryFile() as file_handle:
        save_local_manifest(MANIFEST_DATA, file_handle.name)

        with open(file_handle.name, "rb") as reader:
            data = reader.read()

    # NOTE: This will need updating when erlang library Atom handling is fixed to match Otp behaviour
    assert (
        data
        == b"\x83l\x00\x00\x00\x03h\x04b\x00\x00\x03\xcck\x00j/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488ags\x0friak@172.17.0.2\x00\x00\x03`\x00\x00\x00\x03\x02h\x03b\x00\x00\x05\xb1s\x04stndh\x04s\x05o_rkvh\x02m\x00\x00\x00\x08testTypem\x00\x00\x00\x0btypedBucketm\x00\x00\x00\x0btypedKey984s\x04nullh\x04b\x00\x00\x01\xe6k\x00j/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/486_71e4b785-9c5d-4f3a-bb4a-d2e3fdc66945gs\x0friak@172.17.0.2\x00\x00G\xe4\x00\x00\x00\x01\x02h\x03b\x00\x00\x03\xcbs\x04stndh\x04s\x05o_rkvh\x02m\x00\x00\x00\x08testTypem\x00\x00\x00\x0btypedBucketm\x00\x00\x00\x0btypedKey984s\x04nullh\x04a\x00k\x00h/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/0_50f4666b-6ad8-4b6f-9e2a-23a235c82706gs\x0friak@172.17.0.2\x00\x00\x03\xb0\x00\x00\x00\x00\x02h\x03b\x00\x00\x01\xe5s\x04stndh\x04s\x05o_rkvh\x02m\x00\x00\x00\x08testTypem\x00\x00\x00\x0btypedBucketm\x00\x00\x00\x0btypedKey984s\x04nullj"
    )


def test_upload_new_manifest(s3_client):
    manifest_name = upload_new_manifest(MANIFEST_DATA, "0", "s3://test/upload_new_manifest", None)

    s3_obj = s3_client.get_object(Bucket="test", Key="upload_new_manifest/0/journal/journal_manifest/0.man")

    assert manifest_name.split("#")[0] == "s3://test/upload_new_manifest/0/journal/journal_manifest/0.man"
    assert manifest_name.split("#")[1] == s3_obj["VersionId"]
    s3_data = s3_obj["Body"].read()
    # NOTE: This will need updating when erlang library Atom handling is fixed to match Otp behaviour
    assert (
        s3_data
        == b"\x83l\x00\x00\x00\x03h\x04b\x00\x00\x03\xcck\x00j/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488ags\x0friak@172.17.0.2\x00\x00\x03`\x00\x00\x00\x03\x02h\x03b\x00\x00\x05\xb1s\x04stndh\x04s\x05o_rkvh\x02m\x00\x00\x00\x08testTypem\x00\x00\x00\x0btypedBucketm\x00\x00\x00\x0btypedKey984s\x04nullh\x04b\x00\x00\x01\xe6k\x00j/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/486_71e4b785-9c5d-4f3a-bb4a-d2e3fdc66945gs\x0friak@172.17.0.2\x00\x00G\xe4\x00\x00\x00\x01\x02h\x03b\x00\x00\x03\xcbs\x04stndh\x04s\x05o_rkvh\x02m\x00\x00\x00\x08testTypem\x00\x00\x00\x0btypedBucketm\x00\x00\x00\x0btypedKey984s\x04nullh\x04a\x00k\x00h/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/0_50f4666b-6ad8-4b6f-9e2a-23a235c82706gs\x0friak@172.17.0.2\x00\x00\x03\xb0\x00\x00\x00\x00\x02h\x03b\x00\x00\x01\xe5s\x04stndh\x04s\x05o_rkvh\x02m\x00\x00\x00\x08testTypem\x00\x00\x00\x0btypedBucketm\x00\x00\x00\x0btypedKey984s\x04nullj"
    )


def test_upload_manifests(s3_client):
    manifests = [
        "s3://example/path1#version1",
        "s3://example/path2#version2",
        "s3://example/path3#version3",
        "s3://example/path4#version4",
        "s3://example/path5#version5",
    ]
    upload_manifests(manifests, "s3://test/upload_manifests", None)
    manifest_data = s3_client.get_object(Bucket="test", Key="upload_manifests/MANIFESTS")

    assert (
        manifest_data["Body"].read()
        == b"s3://example/path1#version1\ns3://example/path2#version2\ns3://example/path3#version3\ns3://example/path4#version4\ns3://example/path5#version5"
    )


def test_get_manifests_versions(s3_client):
    v1_response = s3_client.put_object(Bucket="test", Key="get_manifests_versions/MANIFESTS", Body=b"testbytesv1")
    v2_response = s3_client.put_object(Bucket="test", Key="get_manifests_versions/MANIFESTS", Body=b"testbytesv2")
    v3_response = s3_client.put_object(Bucket="test", Key="get_manifests_versions/MANIFESTS", Body=b"testbytesv3")
    v4_response = s3_client.put_object(Bucket="test", Key="get_manifests_versions/MANIFESTS", Body=b"testbytesv4")

    versions = get_manifests_versions("s3://test/get_manifests_versions", None)

    assert [x["VersionId"] for x in versions] == [
        v4_response["VersionId"],
        v3_response["VersionId"],
        v2_response["VersionId"],
        v1_response["VersionId"],
    ]


def test_get_manifests(s3_client):
    manifests_filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data/MANIFESTS")
    s3_client.upload_file(manifests_filename, "test", "get_manifests/MANIFESTS")
    response = s3_client.head_object(Bucket="test", Key="get_manifests/MANIFESTS")

    manifests = get_manifests("s3://test/get_manifests", response["VersionId"], None)

    assert len(manifests) == 64
    assert manifests[0] == (
        "s3://test/hotbackup3/639406966332270026714112114313373821099470487552/journal/journal_manifest/0.man",
        "b313eeee-c80d-4a5f-ade9-7630ffe7853c",
    )
    assert manifests[63] == (
        "s3://test/hotbackup3/730750818665451459101842416358141509827966271488/journal/journal_manifest/0.man",
        "59ed5ac4-73d3-48e3-a4aa-50b94e77c69c",
    )


def test_get_manifests_path():
    assert get_manifests_path("/example/path") == "/example/path/MANIFESTS"
    assert get_manifests_path("s3:///bucket/path") == "s3:///bucket/path/MANIFESTS"
