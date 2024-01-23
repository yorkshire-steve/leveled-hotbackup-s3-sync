import os

import pytest
import urllib3
from botocore.session import Session
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from leveled_hotbackup_s3_sync.s3mmap import S3FileReader

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


@pytest.fixture(autouse=True)
def reset_s3_fixture():
    # We reuse the MotoServer for all tests
    # But we do want a clean state for every test
    urllib3.HTTPConnectionPool(host="127.0.0.1", port=5555).request("POST", f"{ENDPOINT_URI}/moto-api/reset")


@pytest.fixture(name="s3_client")
def fixture_s3_client(s3_base):  # pylint: disable=unused-argument
    # NB: we use the sync botocore client for setup
    session = Session()
    s3_client = session.create_client("s3", region_name="us-east-1", endpoint_url=ENDPOINT_URI)
    s3_client.create_bucket(Bucket="test")
    yield s3_client


def test_s3filereader(s3_client):
    s3_client.put_object(Bucket="test", Key="testreader", Body=b"testbody")

    reader = S3FileReader("s3://test/testreader", endpoint=ENDPOINT_URI)

    assert len(reader) == 8
    assert reader[1:3] == b"es"
    assert reader[1] == b"e"
    assert reader[2:] == b"stbody"
    assert reader[:3] == b"tes"
    with pytest.raises(NotImplementedError):
        _ = reader[::2]
    with pytest.raises(NotImplementedError):
        _ = reader[-1:]
    with pytest.raises(NotImplementedError):
        _ = reader[:-1]
