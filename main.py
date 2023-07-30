# This is a sample Python script.
import os

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import re
import config_factory as cf
import datetime as dt
import logging

scheduler = BlockingScheduler()

FORMAT = '%(asctime)s  %(message)s'
logging.basicConfig(format=FORMAT, level=logging.DEBUG)

log = logging.getLogger(__name__)


def get_oldest_file(database_name, base_dir):
    filename = None
    oldest_date = None
    for f in os.listdir(base_dir):
        date_str = f.replace(database_name, "")
        date_str = date_str.replace("_", "")
        date_str = date_str.replace(".gz", "")
        date_real = dt.datetime.strptime(date_str, '%Y%m%d%H%M%S')
        if oldest_date is None:
            oldest_date = date_real
            filename = f
            continue
        if date_real < oldest_date:
            oldest_date = date_real
            filename = f
    return filename


def backup():
    print("Starting backup")
    config = cf.get_or_build_config()

    user = cf.get_config_value(config, cf.CONFIG_MYSQL, 'user')
    host = cf.get_config_value(config, cf.CONFIG_MYSQL, 'host')
    passwd = cf.get_config_value(config, cf.CONFIG_MYSQL, 'passwd')
    database = cf.get_config_value(config, cf.CONFIG_MYSQL, 'database')

    base_dir = cf.get_config_value(config, cf.CONFIG_DEFAULT, 'base_dir')
    if not os.path.exists(base_dir):
        os.mkdir(base_dir)

    files = os.listdir(base_dir)
    count = 0
    for f in files:
        if f.startswith(database) and f.endswith("gz"):
            count = count + 1

    if count >= cf.get_config_value(config, cf.CONFIG_MYSQL, 'max_files'):
        log.info("Max file reached")
        oldest_file = get_oldest_file(database, base_dir)
        log.info(f"Removing... {oldest_file}")
        os.remove(os.path.join(base_dir, oldest_file))

    filename = f"{database}_{dt.datetime.now().strftime('%Y%m%d%H%M%S')}.gz"

    log.info("Backupiando...")
    # command = f"mysqldump -u {user} -p{passwd} -h {host} --column-statistics=0 -A -R -E --triggers --single-transaction | gzip > {os.path.join(base_dir, filename)}"
    command = f"mysqldump -u {user} -p{passwd} -h {host}  -A -R -E --triggers --single-transaction | gzip > {os.path.join(base_dir, filename)}"
    if cf.get_config_value(config, cf.CONFIG_DEFAULT, 'test_run'):
        with open(os.path.join(base_dir, filename), 'w', encoding="utf-8") as filew:
            filew.write("OPA")
            log.info(command)
            return
    log.info("Running command....")
    log.info(command)
    os.system(command)
    log.info("Finished!")


def verify_cron_expression(text):
    validate_crontab_time_format_regex = re.compile(
        "{0}\s+{1}\s+{2}\s+{3}\s+{4}".format(
            "(?P<minute>\*|[0-5]?\d)",
            "(?P<hour>\*|[01]?\d|2[0-3])",
            "(?P<day>\*|0?[1-9]|[12]\d|3[01])",
            "(?P<month>\*|0?[1-9]|1[012])",
            "(?P<day_of_week>\*|[0-6](\-[0-6])?)"
        )  # end of str.format()
    )  # end of re.compile()
    return validate_crontab_time_format_regex.match(text)


def run_scheduler():
    log.info("Starting Scheduler")

    cfg = cf.get_or_build_config()
    scheduler_count = 0
    for key, value in cfg[cf.CONFIG_CRONS].items():
        if value is None:
            continue

        clean_value = cf.get_config_value(cfg, cf.CONFIG_CRONS, key)
        log.info(f"Adding {key}, with {value}")
        trigger = CronTrigger.from_crontab(clean_value)
        log.info(f"Next execution of {key} ({clean_value}) is {trigger.get_next_fire_time(None, dt.datetime.now())}")
        scheduler.add_job(backup, trigger=trigger, name=key)
        scheduler_count = scheduler_count + 1

    if scheduler_count > 0:
        scheduler.start()


if __name__ == '__main__':
    # backup()
    run_scheduler()
