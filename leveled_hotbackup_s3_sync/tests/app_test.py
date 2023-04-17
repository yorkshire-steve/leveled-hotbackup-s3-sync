import os.path
import tempfile
from unittest.mock import patch

import boto3
import pytest
from moto import mock_s3

from leveled_hotbackup_s3_sync.app import (
    backup,
    check_endpoint_url,
    check_s3_url,
    list_versions,
    main,
    restore,
)
from leveled_hotbackup_s3_sync.journal import list_keys
from leveled_hotbackup_s3_sync.manifest import read_manifest
from leveled_hotbackup_s3_sync.utils import swap_path


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


def test_backup(s3_client):
    response = s3_client.list_objects_v2(Bucket="test", Prefix="hotbackup3/")
    assert "Contents" not in response
    backup("/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59", "s3://test/hotbackup3", False, None)
    response = s3_client.list_objects_v2(Bucket="test", Prefix="hotbackup3/")
    assert len(response["Contents"]) == 215

    manifests_obj = s3_client.get_object(Bucket="test", Key="hotbackup3/MANIFESTS")
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data/MANIFESTS"), "rb") as file_handle:
        manifests_file = file_handle.read()

    s3_manifest_names = [x.split("#")[0] for x in manifests_obj["Body"].read().decode("utf-8").split("\n")]
    file_manifest_names = [x.split("#")[0] for x in manifests_file.decode("utf-8").split("\n")]

    assert s3_manifest_names == file_manifest_names


def test_restore(s3_client):
    backup("/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59", "s3://test/hotbackup3", False, None)
    response = s3_client.head_object(Bucket="test", Key="hotbackup3/MANIFESTS")

    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data/MANIFESTS"), "rb") as file_handle:
        manifests_file = file_handle.read()
    file_manifest_names = [x.split("#")[0] for x in manifests_file.decode("utf-8").split("\n")]

    with tempfile.TemporaryDirectory() as tmpdir:
        restore("s3://test/hotbackup3", response["VersionId"], tmpdir, None)

        assert len(os.listdir(tmpdir)) == 64

        for manifest_filename in file_manifest_names:
            assert os.path.exists(swap_path(manifest_filename, "s3://test/hotbackup3", tmpdir)) is True

        manifest0 = read_manifest(os.path.join(tmpdir, "0/journal/journal_manifest/0.man"))
        assert len(manifest0) == 3
        assert manifest0[0][1] == os.path.join(
            tmpdir, "0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a"
        ).encode("utf-8")
        journal_keys = list_keys(
            os.path.join(tmpdir, "0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a.cdb")
        )
        assert len(journal_keys) == 485


def test_list_versions(s3_client, capsys):
    version1 = s3_client.put_object(Bucket="test", Key="list_versions/MANIFESTS", Body=b"version1")
    version2 = s3_client.put_object(Bucket="test", Key="list_versions/MANIFESTS", Body=b"version2")
    version3 = s3_client.put_object(Bucket="test", Key="list_versions/MANIFESTS", Body=b"version3")

    list_versions("s3://test/list_versions", None)

    captured = capsys.readouterr()
    version_list = captured.out.split("\n")
    version_list.remove("")
    assert version1["VersionId"] in version_list[2]
    assert version2["VersionId"] in version_list[1]
    assert version3["VersionId"] in version_list[0]
    assert len(version_list) == 3


def test_check_s3_url():
    assert check_s3_url("s3://test/path/key") == "s3://test/path/key"
    with pytest.raises(ValueError):
        check_s3_url("randomtext")
    with pytest.raises(ValueError):
        check_s3_url("/file/path")
    with pytest.raises(ValueError):
        check_s3_url("https://www.google.com")


def test_check_endpoint_url():
    assert check_endpoint_url("http://localhost") == "http://localhost"
    with pytest.raises(ValueError):
        check_endpoint_url("http://localhost/path")


@patch("leveled_hotbackup_s3_sync.app.backup")
@patch("argparse._sys.argv", new=["python", "--local", "/local/path", "--s3", "s3://bucket/path"])
def test_main_backup(patched_backup):
    main()
    patched_backup.assert_called_with("/local/path", "s3://bucket/path", False, None)


@patch("leveled_hotbackup_s3_sync.app.backup")
@patch(
    "argparse._sys.argv",
    new=["python", "--local", "/local/path", "--s3", "s3://bucket/path", "-a", "backup", "--hintsfiles"],
)
def test_main_backup_with_hints(patched_backup):
    main()
    patched_backup.assert_called_with("/local/path", "s3://bucket/path", True, None)


@patch("leveled_hotbackup_s3_sync.app.backup")
@patch("argparse._sys.argv", new=["python", "--s3", "s3://bucket/path"])
def test_main_backup_no_local(_patched_backup):
    with pytest.raises(ValueError) as exc:
        main()
    assert str(exc.value) == "Must specify local directory to backup from"


@patch("leveled_hotbackup_s3_sync.app.restore")
@patch(
    "argparse._sys.argv",
    new=["python", "--local", "/local/path", "--s3", "s3://bucket/path", "-v", "VERSIONID", "-a", "restore"],
)
def test_main_restore(patched_restore):
    main()
    patched_restore.assert_called_with("s3://bucket/path", "VERSIONID", "/local/path", None)


@patch("leveled_hotbackup_s3_sync.app.restore")
@patch(
    "argparse._sys.argv",
    new=["python", "--s3", "s3://bucket/path", "-v", "VERSIONID", "-a", "restore"],
)
def test_main_restore_no_local(_patched_restore):
    with pytest.raises(ValueError) as exc:
        main()
    assert str(exc.value) == "Must specify local directory to restore to"


@patch("leveled_hotbackup_s3_sync.app.restore")
@patch(
    "argparse._sys.argv",
    new=["python", "--local", "/local/path", "--s3", "s3://bucket/path", "-a", "restore"],
)
def test_main_restore_no_version(_patched_restore):
    with pytest.raises(ValueError) as exc:
        main()
    assert str(exc.value) == "Must specify VersionId of MANIFESTS file to restore from"


@patch("leveled_hotbackup_s3_sync.app.list_versions")
@patch("argparse._sys.argv", new=["python", "-s", "s3://bucket/path", "-a", "list"])
def test_main_list_verions(patched_list_versions):
    main()
    patched_list_versions.assert_called_with("s3://bucket/path", None)


@patch("leveled_hotbackup_s3_sync.app.list_versions")
@patch("argparse._sys.argv", new=["python", "-s", "s3://bucket/path", "-a", "list", "-e", "http://localhost"])
def test_main_list_verions_endpoint(patched_list_versions):
    main()
    patched_list_versions.assert_called_with("s3://bucket/path", "http://localhost")
