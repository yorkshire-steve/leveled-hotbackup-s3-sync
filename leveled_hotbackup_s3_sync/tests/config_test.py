import tempfile

import pytest

from leveled_hotbackup_s3_sync.config import (
    check_directory,
    check_endpoint_url,
    check_s3_url,
    read_config,
)


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


def test_check_directory():
    with tempfile.TemporaryDirectory() as temp_directory:
        assert check_directory(temp_directory) == temp_directory
    with pytest.raises(ValueError):
        check_directory("/random/path/shouldnt/exist")


def test_config_1():
    with tempfile.NamedTemporaryFile() as file_handle:
        file_handle.write(EXAMPLE_CONFIG_1)
        file_handle.flush()
        config = read_config(file_handle.name, "123")
    assert config["hotbackup_path"] == "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59"
    assert config["ring_path"] == "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59-ring"
    assert config["leveled_path"] == "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59"
    assert config["s3_path"] == "s3://test/hotbackup/"
    assert config["hints_files"]
    assert config["s3_endpoint"] == "http://localhost:4566"
    assert config["tag"] == "123"


def test_config_2():
    with tempfile.NamedTemporaryFile() as file_handle:
        file_handle.write(EXAMPLE_CONFIG_2)
        file_handle.flush()
        config = read_config(file_handle.name, "123")
    assert config["hotbackup_path"] == "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59"
    assert config["ring_path"] == "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59-ring"
    assert config["leveled_path"] == "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59"
    assert config["s3_path"] == "s3://test2/hotbackup/"
    assert config["hints_files"] is False
    assert config["s3_endpoint"] is None
    assert config["tag"] == "123"


def test_config_3():
    with tempfile.NamedTemporaryFile() as file_handle:
        file_handle.write(EXAMPLE_CONFIG_3)
        file_handle.flush()
        with pytest.raises(ValueError) as exc:
            read_config(file_handle.name, "123")
        assert str(exc.value) == "hotbackup_path is a required parameter in the config file"


def test_config_4():
    with tempfile.NamedTemporaryFile() as file_handle:
        file_handle.write(EXAMPLE_CONFIG_3)
        file_handle.flush()
        with pytest.raises(ValueError) as exc:
            read_config(file_handle.name, "!")
        assert str(exc.value) == "tag must be an alphanumeric string"


EXAMPLE_CONFIG_1 = b"""
hotbackup_path = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59"
ring_path = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59-ring"
leveled_path = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59"
s3_path = "s3://test/hotbackup/"
hints_files = true
s3_endpoint = "http://localhost:4566"
"""

EXAMPLE_CONFIG_2 = b"""
hotbackup_path = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59"
ring_path = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59-ring"
leveled_path = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59"
s3_path = "s3://test2/hotbackup/"
"""

EXAMPLE_CONFIG_3 = b"""
ring_path = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59-ring"
leveled_path = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59"
s3_path = "s3://test/hotbackup/"
hints_files = true
s3_endpoint = "http://localhost:4566"
"""
