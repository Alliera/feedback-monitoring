import os
from datetime import datetime, timedelta
from time import sleep

import requests
import yaml
from pymongo import MongoClient
from pymongo.collection import Collection
from abc import ABC, abstractmethod


class SyncInterface(ABC):
    def __init__(self):
        self.mongo_client = MongoClient(
            os.environ['MONGO_HOST'], int(os.environ['MONGO_PORT']))
        self.db = self.mongo_client.feedback_monitoring
        self.collection = self.get_target_collection()
        self.slug = None
        self.enterprise_id = None
        self.host = None
        self.jwt = None
        self.access_code = None
        self.days_before = 3

    def init(self, enterprise, slug):
        self.host = f"https://{slug}-rest.sandsiv.com" + '/api/rest/'
        self.access_code = enterprise['access_code']
        self.slug = slug
        self.enterprise_id = enterprise['enterprise_id']

    @abstractmethod
    def get_name(self) -> str:
        raise

    @abstractmethod
    def sync(self):
        raise

    @abstractmethod
    def get_target_collection(self) -> Collection:
        raise

    def get_date_from(self, last_date):
        return (last_date - timedelta(days=self.days_before)).strftime("%Y-%m-%d")

    def retake_jwt(self):
        print('Jwt retry by access code...')
        url = f'https://{self.slug}-rest.sandsiv.com/acquire-jwt/?ac={self.access_code}'
        r = requests.get(url).json()
        self.jwt = r['token']

    def request(self, route, query):
        url = f'{self.host}{route}?{query}'
        r = requests.get(url, headers={'authorization': f'jwt {self.jwt}'})
        if r.status_code != 200:
            self.retake_jwt()
            sleep(1)
            return self.request(route, query)
        return r

    def get_db_last_date(self, column):
        last = self.collection.find_one(
            {'enterprise_id': self.enterprise_id}, sort=[(column, -1)])
        if not last:
            return None
        else:
            return last[column]

    @staticmethod
    def get_config():
        with open("enterprises.yaml", 'r') as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

    def get_date_range(self, date_from, date_to):
        start_date = datetime.strptime(date_from, '%Y-%m-%d')
        end_date = datetime.strptime(date_to, '%Y-%m-%d')
        result = []
        for single_date in self.daterange(start_date, end_date):
            result.append(single_date.strftime("%Y-%m-%d"))
        return result

    def daterange(self, start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)
