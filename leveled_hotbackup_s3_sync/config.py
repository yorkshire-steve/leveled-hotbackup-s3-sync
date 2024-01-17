import os
import sys
from urllib.parse import urlparse

from leveled_hotbackup_s3_sync.utils import find_latest_ring

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


def check_s3_url(url: str) -> str:
    parsed_url = urlparse(url)
    if parsed_url.scheme != "s3":
        raise ValueError(f"{url} is not a valid S3 URI")
    return url


def check_endpoint_url(url: str) -> str:
    parsed_url = urlparse(url)
    if parsed_url.path != "":
        raise ValueError(f"{url} is not a valid endpoint URL")
    return url


def check_directory(path: str) -> str:
    parsed_path = os.path.abspath(path)
    if not os.path.isdir(parsed_path):
        raise ValueError(f"{path} does not exist")
    return parsed_path


CONFIG_PARAMETERS = {
    "hotbackup_path": {"required": True, "type": check_directory},
    "ring_path": {"required": True, "type": check_directory},
    "leveled_path": {"required": True, "type": check_directory},
    "s3_path": {"required": True, "type": check_s3_url},
    "hints_files": {"required": False, "type": bool, "default": False},
    "s3_endpoint": {"required": False, "type": check_endpoint_url, "default": None},
}


def read_config(config_filename: str, tag: str) -> dict:
    if not tag.isalnum():
        raise ValueError("tag must be an alphanumeric string")
    config = {}
    with open(config_filename, "rb") as file_handle:
        config_data = tomllib.load(file_handle)
    for param, param_config in CONFIG_PARAMETERS.items():
        if param in config_data:
            param_type = param_config["type"]
            value = param_type(config_data[param])  # type: ignore
            config[param] = value
        else:
            if param_config["required"]:
                raise ValueError(f"{param} is a required parameter in the config file")
            config[param] = param_config["default"]
    config["ring_filename"] = find_latest_ring(config["ring_path"])
    config["tag"] = tag
    return config
