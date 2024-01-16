import tempfile

from leveled_hotbackup_s3_sync.config import read_config


def test_config_1():
    with tempfile.NamedTemporaryFile() as file_handle:
        file_handle.write(EXAMPLE_CONFIG_1)
        file_handle.flush()
        config = read_config(file_handle.name)
    assert config["hotbackup_path"] == "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59"
    assert config["ring_path"] == "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59-ring"
    assert config["leveled_path"] == "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59-leveled"
    assert config["s3_path"] == "s3://test/hotbackup/"
    assert config["hints_files"]
    assert config["s3_endpoint"] == "http://localhost:4566"


def test_config_2():
    with tempfile.NamedTemporaryFile() as file_handle:
        file_handle.write(EXAMPLE_CONFIG_2)
        file_handle.flush()
        config = read_config(file_handle.name)
    assert config["hotbackup_path"] == "/tmp/hotbackup"
    assert config["ring_path"] == "/tmp/ring"
    assert config["leveled_path"] == "/tmp/leveled"
    assert config["s3_path"] == "s3://test2/hotbackup/"
    assert config["hints_files"] is False
    assert config["s3_endpoint"] is None


EXAMPLE_CONFIG_1 = b"""
hotbackup_path = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59"
ring_path = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59-ring"
leveled_path = "/tmp/a5017381-4c3e-46e6-bd02-342c4b894b59-leveled"
s3_path = "s3://test/hotbackup/"
hints_files = true
s3_endpoint = "http://localhost:4566"
"""

EXAMPLE_CONFIG_2 = b"""
hotbackup_path = "/tmp/hotbackup"
ring_path = "/tmp/ring"
leveled_path = "/tmp/leveled"
s3_path = "s3://test2/hotbackup/"
"""
