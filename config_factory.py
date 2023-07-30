import configparser
import os
from enum import Enum
import yaml

CONFIG_DEFAULT = "default"
CONFIG_CRONS = "cron"
CONFIG_MYSQL = "mysql"
CONFIG_MQTT = "mqtt"


def write_yaml(yml_file_path, value: dict):
    with open(yml_file_path, 'w') as yml_out:
        yaml.dump(value, yml_out)


def load_yaml(yml_file_path):
    with open(yml_file_path, 'r') as yml_file:
        return yaml.load(yml_file, Loader=yaml.Loader)


class ValueType(Enum):
    STRING = 'str',
    BOOLEAN = 'bool'
    INT = 'int'


def get_defaults():
    out = {
        CONFIG_DEFAULT: {
            'device_id': "py-mysql-backuper",
            'base_dir': "backups",
            'test_run': True,
            'only_one_run': False
        },
        CONFIG_MYSQL: {
            'host': "127.0.0.1",
            'port': 3306,
            'user': "root",
            'passwd': "root",
            'database': "default",
            'max_files': 10
        },
        CONFIG_CRONS: {},
        CONFIG_MQTT: {
            'enabled': False,
            'host': "127.0.0.1",
            'port': 1883,
            'user': "root",
            'passwd': "root",
            'send_topic': "events/mysql/backup_start"
        }

    }
    for i in range(10):
        if not i == 0:
            out[CONFIG_CRONS][f"expression_{i}"] = None
            continue
        out[CONFIG_CRONS][f"expression_{i}"] = "0 0 * * *"

    return out


def proccess_user_config(user_config):
    hot_config = {}
    base_config = get_defaults()
    for key in base_config.keys():
        hot_config[key] = {}
        for subkey, subvalue in base_config[key].items():
            if key in user_config and subkey in user_config[key]:
                hot_config[key][subkey] = user_config[key][subkey]
    return hot_config


def get_or_build_config():
    result_config = {}
    config_default = get_defaults()
    config_base_path = "configs"
    if not os.path.exists(config_base_path):
        os.mkdir(config_base_path)
    write_yaml(os.path.join(config_base_path, "default_config.yaml"), config_default)

    user_config_path = os.path.join(config_base_path, 'config.yaml')
    env_vars = proccess_env_vars()
    if os.path.exists(user_config_path):
        user_config = load_yaml(user_config_path)
        user_config_merged = merge_dicts(config_default, user_config)
        env_vars_merged = merge_dicts(user_config_merged, env_vars)
        return env_vars_merged
    merged = merge_dicts(config_default, env_vars)
    return merged


def merge_dicts(base: dict, delta: dict):
    result_dict = {}
    for key in base.keys():
        for subkey, subvalue in base[key].items():
            result_dict[key] = {}
            result_dict[key] = base[key]
            if key in delta and subkey in delta[key]:
                result_dict[key][subkey] = delta[key][subkey]
                continue
            result_dict[key][subkey] = subvalue
    return result_dict


def proccess_env_vars():
    hot_config = {}
    base_config = get_defaults()
    for key in base_config.keys():
        for subkey, subvalue in base_config[key].items():
            env_key = f"{key}_{subkey}".upper()
            hot_value = os.getenv(env_key, None)
            if hot_value is None:
                continue
            if key not in hot_config:
                hot_config[key] = {}

            if subvalue is None:
                hot_config[key][subkey] = hot_value
                continue

            if type(subvalue) is int:
                hot_config[key][subkey] = int(hot_value)
                continue

            if type(subvalue) is bool:
                hot_config[key][subkey] = bool(hot_value)
                continue

            hot_config[key][subkey] = hot_value

    return hot_config


def get_config_value(config, segment: str, key: str):
    if segment not in config or key not in config[segment]:
        return None
    return config[segment][key]


def get_config_value_old(config, segment, key):
    if not config:
        return None
    if not segment in config:
        return None

    if not key in config[segment]:
        return None

    value: str = config[segment][key]
    if value.startswith('"') and value.endswith('"'):
        return value.replace('"', '').replace('"', '')

    if 'true' == value:
        return True

    if 'false' == value:
        return False

    return int(value)


def list_config(config):
    for key, value in config[CONFIG_DEFAULT].items():
        print(key, value)


def list_config_by_segment(config, segment):
    for key, value in config[segment].items():
        print(key, value)


if __name__ == '__main__':
    print(get_or_build_config())
