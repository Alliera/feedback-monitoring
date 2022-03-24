import os
import time
from timeloop import Timeloop
from datetime import timedelta
from time import sleep

from mongo_to_elastic import MongoElastic
from sync_interface import SyncInterface
from export_bot import ExportBot
import import_bot

tl = Timeloop()
export_stat = ExportBot()
import_stat = import_bot.ImportBot()


@tl.job(interval=timedelta(seconds=1))
def _export():
    run_command(export_stat)


@tl.job(interval=timedelta(seconds=1))
def _import():
    run_command(import_stat)


@tl.job(interval=timedelta(seconds=1))
def _import():
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
    sleep(1000)


def run_command(bot):
    if isinstance(bot, SyncInterface):
        print("Start sync " + bot.get_name())
        print("Current time : {}".format(time.ctime()))
        try:
            bot.sync()
        except Exception as e:
            print("ERROR----------> for " + bot.get_name() + ": " + str(e))
        print("Sync " + bot.get_name() + " complete")
        print("Current time : {}".format(time.ctime()))


if __name__ == "__main__":
    tl.start(block=True)
