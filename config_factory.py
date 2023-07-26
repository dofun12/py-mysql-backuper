import configparser
import os

CONFIG_DEFAULT = "DEFAULT"
CONFIG_CRONS = "CRON"
CONFIG_MYSQL = "MYSQL"


def generate_config_ini():
    import os
    initial_path = os.path.join('.', 'config.ini')
    if not os.path.exists(initial_path):
        return build_config_default(initial_path)
    read_config = configparser.ConfigParser()
    read_config.read(initial_path)
    return read_config


def add_config(config, segment, key, value, isint=False):
    if isint:
        config[segment][key] = value
        return
    config[segment][key] = f"\"{value}\""


def build_config_default(initial_path):
    _config = configparser.ConfigParser()
    _config[CONFIG_DEFAULT] = {}
    add_config(_config, CONFIG_DEFAULT, 'device_id', "py-mysql-backuper")
    add_config(_config, CONFIG_DEFAULT, 'base_dir', "backups")

    test_run = os.getenv("test_run", False)
    add_config(_config, CONFIG_DEFAULT, 'test_run', test_run)


    _config[CONFIG_CRONS] = {}

    count = 0
    for cron in range(10):
        key = f'cron_backup_{cron}'
        value = os.getenv(key, None)
        if value is None:
            continue
        add_config(_config, CONFIG_CRONS, key, value)
        count = count + 1

    if count < 1:
        _config[CONFIG_CRONS]['cron_backup_1'] = "0 20 * * *"

    _config[CONFIG_MYSQL] = {}
    add_config(_config, CONFIG_MYSQL, 'host', "127.0.0.1")
    add_config(_config, CONFIG_MYSQL, 'user', "root")
    add_config(_config, CONFIG_MYSQL, 'passwd', "root")
    add_config(_config, CONFIG_MYSQL, 'database', "default")
    add_config(_config, CONFIG_MYSQL, 'max_files', "10", True)

    with open(initial_path, 'w') as out_file:
        _config.write(out_file)
    return _config


def get_config_value(config, segment, key):
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
