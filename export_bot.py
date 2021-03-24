from datetime import datetime, timedelta
from pymongo.collection import Collection
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
            last_date = self.get_last_date()
            if last_date:
                date_from = (last_date - timedelta(days=self.days_before)).strftime("%Y-%m-%d")
            else:
                first_export_date_str = self.request('export/', "limit=1&sort=id").json()['results'][0]['creation']
                date_from = first_export_date_str.split('T')[0]
            date_to = datetime.now().strftime("%Y-%m-%d")
            last = None
            for date in self.get_date_range(date_from, date_to):
                if last is None:
                    last = date
                    continue
                print(f'Sync export statistics ({slug}) from {last} to {date}...')
                r = self.request('statistics/summary/', f'start_time={last}&range_type=creation&end_time={date}')
                self.__save(datetime.strptime(last, '%Y-%m-%d'), r.json())
                last = date

    def get_target_collection(self) -> Collection:
        return self.db.export_stat

    def get_last_date(self):
        last = self.collection.find_one({'enterprise_id': self.enterprise_id}, sort=[('date', -1)])
        if not last:
            return None
        else:
            return last['date']

    def __save(self, date, data):
        for channel, item in data.items():
            item['date'] = date
            item['enterprise_id'] = self.enterprise_id
            item['slug'] = self.slug
            doc = self.collection.find_one({"date": date, "channel": channel})
            if not doc:
                self.collection.insert_one(item)
            else:
                self.collection.update_one({"_id": doc["_id"]}, {"$set": item})
