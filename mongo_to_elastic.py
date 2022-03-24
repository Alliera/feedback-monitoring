import json

from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from progress.spinner import Spinner
from datetime import datetime, timedelta


class MongoElastic:
    def __init__(self, args):
        # load configuration
        self.mongodb_config = args.get('mongodb_config')
        self.es_config = args.get('es_config')
        # batch setting
        self.chunk_size = args.get('chunk_size', 500)
        self.limit = args.get('limit', None)

        # setup mongo client
        self.mongodb_client = MongoClient(
            self.mongodb_config["uri"]
        )

        # setup elasticsearch client
        self.es_client = Elasticsearch(
            hosts=self.es_config["hosts"],
            http_auth=(
                self.es_config["username"],
                self.es_config["password"],
            ),
        )
        self.last_date = None
        try:
            resp = self.es_client.search(
                index=self.es_config["index_name"],
                size=1,
                sort=f'{self.es_config["date_column"]}:desc'
            )
            self.last_date = self.parse_date(resp['hits']['hits'][0]['_source'][self.es_config["date_column"]])
            self.last_date = self.last_date - timedelta(days=1)
        except Exception:
            pass

    @staticmethod
    def parse_date(date):
        if not date:
            return None
        try:
            return datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f')
        except Exception:
            return datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')

    def _doc_to_json(self, doc):
        doc_str = json.dumps(doc, default=str)
        doc_json = json.loads(doc_str)
        return doc_json

    def es_add_index_bulk(self, docs):
        actions = []
        for doc in docs:
            doc["_id"] = str(doc["_id"])
            doc["_index"] = self.es_config["index_name"]
            doc["date"] = doc["date"] if "date" in doc else doc[self.es_config["date_column"]]
            actions.append(doc)

        response = helpers.bulk(self.es_client, actions)
        return response

    def fetch_docs(self):
        mongodb_query = dict() if not self.last_date else {self.es_config["date_column"]: {'$gte': self.last_date}}
        mongodb_fields = dict()

        database = self.mongodb_client[self.mongodb_config["database"]]
        collection = database[self.mongodb_config["collection"]]

        no_docs = 0
        offset = 0

        spinner = Spinner('Importing... ')
        count = collection.count_documents(mongodb_query)
        one_per = count / 100
        while True:
            """
            Iterate to fetch documents in batch.
            Iteration stops once it hits limit or no document left.
            """
            mongo_cursor = collection.find(mongodb_query, mongodb_fields)
            mongo_cursor.skip(offset)
            mongo_cursor.limit(self.chunk_size)
            docs = list(mongo_cursor)
            # break loop if no more documents found
            if not len(docs):
                break
            yield docs
            # check for number of documents limit, stop if exceed
            no_docs += len(docs)
            if self.limit and no_docs >= self.limit:
                break
            # update offset to fetch next chunk/page
            offset += self.chunk_size
            print(str(round(offset / one_per)) + '%')
            spinner.next()

        self.mongodb_client.close()

    def start(self):
        for docs in self.fetch_docs():
            self.es_add_index_bulk(docs)
