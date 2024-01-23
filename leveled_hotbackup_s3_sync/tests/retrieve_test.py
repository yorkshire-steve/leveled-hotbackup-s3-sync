import os
import tempfile
from copy import deepcopy
from unittest.mock import patch

import cdblib
import pytest
import urllib3
from botocore.session import Session
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from leveled_hotbackup_s3_sync import erlang
from leveled_hotbackup_s3_sync.app import backup
from leveled_hotbackup_s3_sync.retrieve import (
    find_object,
    find_sqn,
    get_cdb_reader,
    main,
    print_sibling,
    retrieve_object,
    write_sibling,
)
from leveled_hotbackup_s3_sync.tests.app_test import (
    TEST_CONFIG_DICT,
    TEST_CONFIG_FILENAME,
    create_test_config,
)
from leveled_hotbackup_s3_sync.utils import RiakObject

PORT = 5555
ENDPOINT_URI = f"http://127.0.0.1:{PORT}"


@pytest.fixture(name="s3_base", scope="module")
def fixture_s3_base():
    # writable local S3 system

    # This fixture is module-scoped, meaning that we can re-use the MotoServer across all tests
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=PORT)
    server.start()
    if "AWS_SECRET_ACCESS_KEY" not in os.environ:
        os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"

    print("server up")
    yield
    print("moto done")
    server.stop()


@pytest.fixture(name="s3_reset")
def reset_s3_fixture():
    # We reuse the MotoServer for all tests
    # But we do want a clean state for every test
    urllib3.HTTPConnectionPool(host="127.0.0.1", port=5555).request("POST", f"{ENDPOINT_URI}/moto-api/reset")


@pytest.fixture(name="s3_client")
def fixture_s3_client(s3_base, s3_reset):  # pylint: disable=unused-argument
    # NB: we use the sync botocore client for setup
    session = Session()
    s3_client = session.create_client("s3", region_name="us-east-1", endpoint_url=ENDPOINT_URI)
    s3_client.create_bucket(Bucket="test")
    yield s3_client


def test_get_cdb_reader(s3_client):
    with tempfile.NamedTemporaryFile() as file_handle:
        with cdblib.Writer(file_handle) as writer:
            writer.putint("key1", 123)
            writer.putint("key2", 1230)
            writer.putint("key3", 456)
        file_handle.flush()

        reader = get_cdb_reader(file_handle.name)
        assert reader.getint("key1") == 123
        assert reader.getint("key2") == 1230
        assert reader.getint("key3") == 456
        assert len(reader.keys()) == 3

        file_handle.seek(0)
        s3_client.put_object(Bucket="test", Key="test.cdb", Body=file_handle.read())

        reader = get_cdb_reader("s3://test/test.cdb", endpoint=ENDPOINT_URI)
        assert reader.getint("key1") == 123
        assert reader.getint("key2") == 1230
        assert reader.getint("key3") == 456
        assert len(reader.keys()) == 3


def test_find_sqn():
    file_handle1 = tempfile.NamedTemporaryFile(suffix=".hints.cdb")
    file_handle2 = tempfile.NamedTemporaryFile(suffix=".hints.cdb")
    with cdblib.Writer(file_handle1) as writer:
        writer.putint(
            erlang.term_to_binary((erlang.OtpErlangBinary(b"testBucket"), erlang.OtpErlangBinary(b"testKey1"))), 123
        )
        writer.putint(
            erlang.term_to_binary((erlang.OtpErlangBinary(b"testBucket"), erlang.OtpErlangBinary(b"testKey2"))), 123
        )
    with cdblib.Writer(file_handle2) as writer:
        writer.putint(
            erlang.term_to_binary((erlang.OtpErlangBinary(b"testBucket"), erlang.OtpErlangBinary(b"testKey2"))), 456
        )
    file_handle1.flush()
    file_handle2.flush()

    filename1 = file_handle1.name[:-10]
    filename2 = file_handle2.name[:-10]
    manifest = [(0, filename2.encode("utf-8")), (0, filename1.encode("utf-8"))]

    assert find_sqn(manifest, b"testBucket", b"testKey1", endpoint=ENDPOINT_URI) == (123, f"{filename1}.cdb")
    assert find_sqn(manifest, b"testBucket", b"testKey2", endpoint=ENDPOINT_URI) == (456, f"{filename2}.cdb")
    assert find_sqn(manifest, b"testBucket", b"nonexistentKey", endpoint=ENDPOINT_URI) == (None, None)

    file_handle1.close()
    file_handle2.close()


def test_find_object():
    filename = (
        "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59/0/journal/journal_files/0_50f4666b-6ad8-4b6f-9e2a-23a235c82706.cdb"
    )
    journal_key = erlang.term_to_binary(
        (
            1,
            erlang.OtpErlangAtom(b"stnd"),
            (
                erlang.OtpErlangAtom(b"o_rkv"),
                erlang.OtpErlangBinary(b"testBucket"),
                erlang.OtpErlangBinary(b"testKey17"),
                erlang.OtpErlangAtom(b"null"),
            ),
        )
    )

    found_object = find_object(filename, journal_key)
    assert isinstance(found_object, RiakObject)
    assert len(found_object.siblings) == 1
    assert found_object.siblings[0]["value"] == b'{"test":"data17"}'

    found_object = find_object(filename, b"doesnotexist")
    assert isinstance(found_object, RiakObject)
    assert len(found_object.siblings) == 0


def test_print_sibling(capsys):
    sibling = {"value": b"testvalue", "metadata": {"last_modified": "1706009850.709926", "vtag": b"12345"}}
    print_sibling(sibling)
    captured = capsys.readouterr()
    assert (
        captured.out
        == """Last Modified: 2024-01-23 11:37:30.709926. Vtag: 12345

Object value:

b'testvalue'

"""
    )


def test_write_sibling():
    sibling = {"value": b"testvalue"}
    with tempfile.NamedTemporaryFile() as file_handle:
        write_sibling(file_handle.name, sibling)
        assert file_handle.read() == b"testvalue"


def test_retrieve_object(s3_client, capsys):  # pylint: disable=unused-argument
    config = deepcopy(TEST_CONFIG_DICT)
    config["s3_endpoint"] = ENDPOINT_URI
    config["hints_files"] = True
    backup(config)

    config["bucket"] = b"testBucket"
    config["key"] = b"testKey1"
    config["buckettype"] = None
    config["output"] = None

    retrieve_object(config)
    captured = capsys.readouterr()

    assert "Found object in journal." in captured.out
    assert 'b\'{"test":"secondUpdate1"}\'' in captured.out

    with tempfile.NamedTemporaryFile() as file_handle:
        config["output"] = file_handle.name
        retrieve_object(config)
        assert file_handle.read() == b'{"test":"secondUpdate1"}'

    config["key"] = b"doesnotexist"
    retrieve_object(config)
    captured = capsys.readouterr()

    assert "Could not find key in hotbackup." in captured.out


@patch("leveled_hotbackup_s3_sync.retrieve.retrieve_object")
@patch(
    "argparse._sys.argv",
    new=["python", "123", "--config", TEST_CONFIG_FILENAME, "--bucket", "testBucket", "--key", "testKey1"],
)
def test_main(patched_retrieve_object):
    with create_test_config():
        main()

    config = deepcopy(TEST_CONFIG_DICT)
    config["bucket"] = b"testBucket"
    config["key"] = b"testKey1"
    config["buckettype"] = None
    config["output"] = None
    patched_retrieve_object.assert_called_with(config)
