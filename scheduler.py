import os
import time
import traceback
from timeloop import Timeloop
from datetime import datetime, timedelta
from time import sleep

from mongo_to_elastic import MongoElastic
from sync_interface import SyncInterface
from export_bot import ExportBot
import import_bot

tl = Timeloop()
export_stat = ExportBot()
import_stat = import_bot.ImportBot()


def is_hour(hour):
    current_time = datetime.now()
    target_time = current_time.replace(hour=int(hour), minute=0, second=0, microsecond=0)
    delta = timedelta(minutes=1)

    return target_time - delta <= current_time <= target_time + delta


@tl.job(interval=timedelta(seconds=10))
def _export():
    if is_hour("1"):
        run_command(export_stat)


@tl.job(interval=timedelta(seconds=10))
def _import():
    if is_hour("1"):
        run_command(import_stat)


@tl.job(interval=timedelta(seconds=10))
def _import_elastic():
    if is_hour("6"):
        mongo_host = os.environ['MONGO_HOST']
        mongo_port = os.environ['MONGO_PORT']
        config = {
            "mongodb_config": {
                "uri": f'mongodb://{mongo_host}:{mongo_port}',
                "database": "feedback_monitoring",
                "collection": "export_stat"
            },
            "es_config": {
                "hosts": ["https://a4e0da1e5c3e4aeda2551efec6dea894.europe-west3.gcp.cloud.es.io:9243"],
                "username": "elastic",
                "password": "zrNEbGgMvWbt8gEoDqUpbWE1",
                "index_name": "export_stat",
                "date_column": "date"
            },
            'chunk_size': 10000,
        }
        obj = MongoElastic(config)
        obj.start()
        config['mongodb_config']['collection'] = 'import_stat'
        config['es_config']['index_name'] = 'import_stat'
        config['es_config']['date_column'] = 'creation'

        obj = MongoElastic(config)
        obj.start()
        sleep(100)


def run_command(bot):
    if isinstance(bot, SyncInterface):
        print("Start sync " + bot.get_name())
        print("Current time : {}".format(time.ctime()))
        try:
            bot.sync()
        except Exception as e:
            print("ERROR----------> for " + bot.get_name() + ": " + traceback.format_exc())
        print("Sync " + bot.get_name() + " complete")
        print("Current time : {}".format(time.ctime()))


if __name__ == "__main__":
    tl.start(block=True)
