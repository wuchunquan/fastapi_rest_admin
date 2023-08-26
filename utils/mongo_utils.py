import json
from copy import deepcopy

from bson import ObjectId
from pydantic import BaseModel
from pymongo import MongoClient
from typing import Generic, TypeVar

ModelType = TypeVar("ModelType", bound=BaseModel)


def pydantic_to_mg_dict(data: BaseModel):
    data = json.loads(data.json(exclude_none=True))
    return data


class MongoConnect:
    def __init__(self, host="localhost", port: int = 27017, db: str = None, user: str = None, password: str = None):
        self.host = host
        self.port = int(port)
        self.db = db
        self.user = user
        self.password = password
        if self.user:
            self.client = MongoClient(host=self.host, port=self.port,
                                      username=self.user, password=self.password)
        else:
            self.client = MongoClient(host=self.host, port=self.port)
        self.db = self.client[self.db]

    def get_db(self):
        yield self.db

    def insert_data(self, dbname: str, sheet: str, data: dict):
        collection = self.client[dbname][sheet]
        item = collection.insert_one(data)
        return item.inserted_id.__str__()

    def delete_data_by_id(self, dbname: str, sheet: str, _id: str):
        collection = self.client[dbname][sheet]
        collection.delete_one({'_id': ObjectId(_id)})
        return True

    def find_data_by_id(self, dbname: str, sheet: str, _id: str):
        collection = self.client[dbname][sheet]
        return collection.find_one({'_id': ObjectId(_id)}, {"_id": False})

    def update_data_by_id(self, dbname: str, sheet: str, _id: str, data: dict):
        collection = self.client[dbname][sheet]
        collection.update_one({'_id': ObjectId(_id)}, {"$set": data})
        return True

    def replace_data_by_id(self, dbname: str, sheet: str, _id: str, data: dict):
        collection = self.client[dbname][sheet]
        collection.find_one_and_replace({'_id': ObjectId(_id)}, data)
        return True
