import sys
from urllib.parse import urlparse

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


def check_s3_url(url: str) -> str:
    parsed_url = urlparse(url)
    if parsed_url.scheme != "s3":
        raise ValueError
    return url


def check_endpoint_url(url: str) -> str:
    parsed_url = urlparse(url)
    if parsed_url.path != "":
        raise ValueError
    return url


CONFIG_PARAMETERS = {
    "hotbackup_path": {"required": True, "type": str},
    "ring_path": {"required": True, "type": str},
    "leveled_path": {"required": True, "type": str},
    "s3_path": {"required": True, "type": check_s3_url},
    "hints_files": {"required": False, "type": bool, "default": False},
    "s3_endpoint": {"required": False, "type": check_endpoint_url, "default": None},
}


def read_config(config_filename: str) -> dict:
    config = {}
    with open(config_filename, "rb") as f:
        config_data = tomllib.load(f)
    for param, param_config in CONFIG_PARAMETERS.items():
        if param in config_data:
            param_type = param_config["type"]
            value = param_type(config_data[param])  # type: ignore
            config[param] = value
        else:
            if param_config["required"]:
                raise ValueError(f"{param} is a required parameter in the config file")
            config[param] = param_config["default"]
    return config
