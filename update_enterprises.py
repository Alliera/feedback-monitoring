import os

from pymongo import MongoClient

from sync_interface import SyncInterface


class EnterpriseUpdater:
    def __init__(self):
        self.mongo_client = MongoClient(
            os.environ['MONGO_HOST'], int(os.environ['MONGO_PORT']))
        self.db = self.mongo_client.feedback_monitoring
        self.collection = self.db.enterprises

    def update(self):
        for slug, enterprise in SyncInterface.get_config().items():
            doc = {'enterprise_id': enterprise['enterprise_id'], 'slug': slug}
            self.collection.update_one({"enterprise_id": doc["enterprise_id"]}, {
                                       "$set": doc}, upsert=True)

    def create_indexes(self):
        self.db.import_stat.create_index([("id", -1), ("slug", -1)])


u = EnterpriseUpdater()
print('update Enterprises')
u.update()
print('create indexes')
u.create_indexes()
print("Done...")