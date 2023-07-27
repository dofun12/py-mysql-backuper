# This is a sample Python script.
import os

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import re
import config_factory as cf
import datetime as dt

scheduler = BlockingScheduler()


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
    config = cf.generate_config_ini()

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
        print("Max file reached")
        oldest_file = get_oldest_file(database,base_dir)
        print(f"Removing... {oldest_file}")
        os.remove(os.path.join(base_dir, oldest_file))

    filename = f"{database}_{dt.datetime.now().strftime('%Y%m%d%H%M%S')}.gz"

    print("Backupiando...")
    #command = f"mysqldump -u {user} -p{passwd} -h {host} --column-statistics=0 -A -R -E --triggers --single-transaction | gzip > {os.path.join(base_dir, filename)}"
    command = f"mysqldump -u {user} -p{passwd} -h {host}  -A -R -E --triggers --single-transaction | gzip > {os.path.join(base_dir, filename)}"
    if cf.get_config_value(config, cf.CONFIG_DEFAULT, 'test_run').lower() == "true":
        with open(os.path.join(base_dir, filename), 'w', encoding="utf-8") as filew:
            filew.write("OPA")
            print(command)
            return
    print("Running command....")
    print(command)
    os.system(command)
    print("Finished!")


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
    print("Starting Scheduler")

    cfg = cf.generate_config_ini()
    cf.list_config(cfg)
    scheduler_count = 0
    for key, value in cfg[cf.CONFIG_CRONS].items():
        clean_value = cf.get_config_value(cfg, cf.CONFIG_CRONS, key)
        if not key.startswith("exp_backup"):
            continue
        if verify_cron_expression(clean_value):
            print(f"Adding {key}, with {value}")
            trigger = CronTrigger.from_crontab(clean_value)
            scheduler.add_job(backup, trigger=trigger)
            scheduler_count = scheduler_count + 1
            continue
        print(f"Invalid expression: {value}")
    if scheduler_count > 0:
        scheduler.print_jobs()
        scheduler.start()


if __name__ == '__main__':
    #backup()
    run_scheduler()
