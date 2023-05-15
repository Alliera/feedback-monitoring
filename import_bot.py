from datetime import datetime

from pymongo.collection import Collection

from sync_interface import SyncInterface
from requests.exceptions import Timeout


class ImportBot(SyncInterface):
    def __init__(self):
        SyncInterface.__init__(self)

    def get_name(self) -> str:
        return 'Import'

    def sync(self):
        for slug, enterprise in self.get_config().items():
            print('==========================================================')
            print(f'Start Import statistics sync for `{slug}` enterprise...')
            print('==========================================================')
            self.init(enterprise, slug)
            last_date = self.get_db_last_date('creation')
            try:
                print(f'Last Import date {last_date} for `{slug}` enterprise...')
                if last_date:
                    date_from = self.get_date_from(last_date)
                else:
                    first_export_date_str = self.request(
                        'v2/import/expand-basic/', "limit=1&sort=id").json()['results'][0]['creation']
                    date_from = first_export_date_str.split('T')[0]
                date_to = datetime.now().strftime("%Y-%m-%d")

                print(f'Import start date FROM {date_from} TO {date_to} for `{slug}` enterprise...')
                print(f'Import First request: page=1&limit=1000&creation_range={date_from + "," + date_to}')
                r = self.request('v2/import/expand-basic/',
                                 f'page=1&limit=1000&creation_range={date_from + "," + date_to}').json()
                self.__save(r['results'])
                while r['next']:
                    parts = r['next'].split('?')
                    r = self.request('v2/import/expand-basic/', parts[1]).json()
                    print(r['next'])
                    self.__save(r['results'])
            except Timeout:
                print(f'Timeout exceeded for request of `{slug}` enterprise...')

    def get_target_collection(self) -> Collection:
        return self.db.import_stat

    def parse_date(self, date):
        if not date:
            return None
        return datetime.fromisoformat(date)

    def __save(self, results):
        print(f'Saving import {len(results)} items for `{self.slug}` enterprise...')
        for r in results:
            doc = {'creation': self.parse_date(r['creation']),
                   'end_time': self.parse_date(r['end_time']),
                   'enterprise_id': self.enterprise_id,
                   'slug': self.slug}
            fields = ['id', 'state', 'total_row_count', 'processed_row_count', 'failed_row_count']
            for filed in fields:
                doc[filed] = r[filed]
            self.collection.update_one({"id": doc["id"], "slug": self.slug}, {"$set": doc}, upsert=True)
