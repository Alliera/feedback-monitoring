from datetime import datetime

from pymongo.collection import Collection

from sync_interface import SyncInterface


class ImportBot(SyncInterface):
    def __init__(self):
        SyncInterface.__init__(self)

    def get_name(self) -> str:
        return 'Export'

    def sync(self):
        for slug, enterprise in self.get_config().items():
            print('==========================================================')
            print(f'Start Import statistics sync for `{slug}` enterprise...')
            print('==========================================================')
            self.init(enterprise, slug)

            r = self.request('v2/import/expand-basic/', 'page=1&limit=1000').json()
            self.__save(r['results'])
            while r['next']:
                parts = r['next'].split('?')
                r = self.request('v2/import/expand-basic/', parts[1]).json()
                print(r['next'])
                self.__save(r['results'])

    def get_target_collection(self) -> Collection:
        return self.db.import_stat

    def __save(self, results):
        for r in results:
            doc = {}
            doc['creation'] = datetime.strptime(r['creation'], '%Y-%m-%dT%H:%M:%S.%f') if r['creation'] else None
            doc['end_time'] = datetime.strptime(r['end_time'], '%Y-%m-%dT%H:%M:%S.%f') if r['end_time'] else None
            fields = ['id', 'state', 'total_row_count', 'processed_row_count', 'failed_row_count']
            for filed in fields:
                doc[filed] = r[filed]
            self.collection.update_one({"id": doc["id"]}, {"$set": doc}, upsert=True)
