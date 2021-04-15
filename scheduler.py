import time
from timeloop import Timeloop
from datetime import timedelta
from sync_interface import SyncInterface
import export_bot
import import_bot
# from dotenv import load_dotenv

# load_dotenv()

tl = Timeloop()
export_stat = export_bot.ExportBot()
import_stat = import_bot.ImportBot()


@tl.job(interval=timedelta(seconds=1))
def _export():
    run_command(export_stat)


@tl.job(interval=timedelta(seconds=1))
def _import():
    run_command(import_stat)


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
