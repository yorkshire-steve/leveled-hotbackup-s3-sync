import os.path
import tempfile
from contextlib import contextmanager
from copy import deepcopy
from unittest.mock import patch

import boto3
import pytest
from moto import mock_s3

from leveled_hotbackup_s3_sync.app import backup, main, restore
from leveled_hotbackup_s3_sync.journal import list_keys
from leveled_hotbackup_s3_sync.manifest import read_manifest
from leveled_hotbackup_s3_sync.utils import get_owned_partitions

TEST_CONFIG_FILENAME = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59.cfg"
TEST_CONFIG = b"""
hotbackup_path = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59"
ring_path = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59-ring"
leveled_path = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59"
s3_path = "s3://test/hotbackup3/"
hints_files = false
"""
TEST_CONFIG_DICT = {
    "hotbackup_path": "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59",
    "ring_path": "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59-ring",
    "ring_filename": "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59-ring/riak_core_ring.default.20240116160656",
    "leveled_path": "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59",
    "s3_path": "s3://test/hotbackup3/",
    "hints_files": False,
    "s3_endpoint": None,
    "tag": "123",
}


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
    backup(TEST_CONFIG_DICT)
    response = s3_client.list_objects_v2(Bucket="test", Prefix="hotbackup3/")
    assert len(response["Contents"]) == 214
    s3_keys = [x["Key"] for x in response["Contents"]]

    partitions = get_owned_partitions(TEST_CONFIG_DICT["ring_filename"])  # type: ignore

    for partition in partitions:
        assert f"hotbackup3/{str(partition)}/journal/journal_manifest/{TEST_CONFIG_DICT['tag']}.man" in s3_keys


def test_restore(s3_client):  # pylint: disable=unused-argument
    config = deepcopy(TEST_CONFIG_DICT)

    backup(config)

    with tempfile.TemporaryDirectory() as tmpdir:
        config["leveled_path"] = tmpdir
        restore(config)

        assert len(os.listdir(tmpdir)) == 64

        partitions = get_owned_partitions(TEST_CONFIG_DICT["ring_filename"])  # type: ignore

        for partition in partitions:
            manifest_filename = os.path.join(tmpdir, str(partition), "journal/journal_manifest/0.man")
            assert os.path.exists(manifest_filename) is True

        manifest0 = read_manifest(os.path.join(tmpdir, "0/journal/journal_manifest/0.man"))
        assert len(manifest0) == 3
        assert manifest0[0][1] == os.path.join(
            tmpdir, "0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a"
        ).encode("utf-8")
        journal_keys = list_keys(
            os.path.join(tmpdir, "0/journal/journal_files/972_e6205c6c-3b8b-40e6-baee-295dcc76488a.cdb")
        )
        assert len(journal_keys) == 485


@contextmanager
def create_test_config():
    with open(TEST_CONFIG_FILENAME, "wb") as file_handle:
        file_handle.write(TEST_CONFIG)
    try:
        yield TEST_CONFIG_FILENAME
    finally:
        os.remove(TEST_CONFIG_FILENAME)


@patch("leveled_hotbackup_s3_sync.app.backup")
@patch("argparse._sys.argv", new=["python", "backup", "123", "--config", TEST_CONFIG_FILENAME])
def test_main_backup(patched_backup):
    with create_test_config():
        main()
    patched_backup.assert_called_with(TEST_CONFIG_DICT)


@patch("leveled_hotbackup_s3_sync.app.restore")
@patch(
    "argparse._sys.argv",
    new=["python", "restore", "123", "--config", TEST_CONFIG_FILENAME],
)
def test_main_restore(patched_restore):
    with create_test_config():
        main()
    patched_restore.assert_called_with(TEST_CONFIG_DICT)
