from datetime import datetime
from pymongo.collection import Collection
from pymongo import ASCENDING

from sync_interface import SyncInterface


class ExportBot(SyncInterface):
    def __init__(self):
        SyncInterface.__init__(self)

    def get_name(self) -> str:
        return 'Export'

    def sync(self):
        for slug, enterprise in self.get_config().items():
            print('==========================================================')
            print(f'Start Export statistics sync for `{slug}` enterprise...')
            print('==========================================================')
            self.init(enterprise, slug)
            last_date = self.get_db_last_date('date')
            print(f'Last Export date {last_date}  for `{slug}` enterprise...')
            if last_date:
                date_from = self.get_date_from(last_date)
            else:
                results = self.request('export/', "limit=1&sort=id").json()['results']
                if not results:
                    continue
                else:
                    first_export_date_str = results[0]['creation']
                    date_from = first_export_date_str.split('T')[0]

            date_to = datetime.now().strftime("%Y-%m-%d")
            last = None
            print(f'Export date from for request: FROM {date_from} TO {date_to} for `{slug}` enterprise...')
            for date in self.get_date_range(date_from, date_to):
                if last is None:
                    last = date
                    continue
                print(
                    f'Sync export statistics ({slug}) from {last} to {date}...')
                r = self.request(
                    'statistics/summary/', f'start_time={last}&range_type=creation&end_time={date}')
                self.__save(datetime.strptime(last, '%Y-%m-%d'), r.json())
                last = date

    def get_target_collection(self) -> Collection:
        return self.db.export_stat

    def __save(self, date, data):
        items = data.items()
        print(f"Saving {len(items)} export items for {date} date for {self.slug}...")
        for channel, doc in items:
            doc['date'] = date
            doc['enterprise_id'] = self.enterprise_id
            doc['slug'] = self.slug
            self.collection.update_one({"date": date, "channel": channel, "slug": self.slug}, {"$set": doc},
                                       upsert=True)

    def create_indexes(self):
        print('start create indexes')
        self.collection.create_index([("date", ASCENDING), ("slug", ASCENDING), ("channel", ASCENDING)])
        print('finish create indexes')
