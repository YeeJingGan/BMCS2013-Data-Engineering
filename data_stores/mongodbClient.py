"""
Author: Jerome Subash A/L Joseph, Gan Yee Jing
"""

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import warnings

class MongoDBClient:
    def __init__(self, 
                 uri = 'mongodb+srv://ganyjwm22:Shen160801!@datapreprocessing.wdbdlb9.mongodb.net/?retryWrites=true&w=majority&appName=DataPreprocessing'):
        
        self.mongodb_client = MongoClient(uri, server_api=ServerApi('1'))
        self._ping_server()

    def _ping_server(self):
        try:
            self.mongodb_client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            warnings.warn(e)

    def list_databases(self):
        return self.mongodb_client.list_database_names()

    def list_collections(self, db_name):
        return self.mongodb_client[db_name].list_collection_names()

    def insert_one(self, db_name, collection_name, document):
        database = self.mongodb_client[db_name]
        collection = database[collection_name]

        try:
            result = collection.insert_many(documents)
            print('Documents successfully inserted:', result.inserted_id)
        except Exception as e:
            warnings.warn(e)

    def insert_many(self, db_name, collection_name, documents):
        database = self.mongodb_client[db_name]
        collection = database[collection_name]

        try:
            result = collection.insert_many(documents)
            print('Documents successfully inserted:', result.inserted_ids)
        except Exception as e:
            warnings.warn(e)

    def read_one(self, db_name, collection_name, query={}):
        database = self.mongodb_client[db_name]
        collection = database[collection_name]

        try:
            return collection.find_one(query)
        except Exception as e:
            warnings.warn(e)

    def read_many(self, db_name, collection_name, query={}):
        database = self.mongodb_client[db_name]
        collection = database[collection_name]

        try:
            return collection.find({},query)
        except Exception as e:
            warnings.warn(e)

    def delete_one(self, db_name, collection_name, query):
        database = self.mongodb_client[db_name]
        collection = database[collection_name]
        collection.delete_one(query)

    def delete_many(self, db_name, collection_name, query):
        database = self.mongodb_client[db_name]
        collection = database[collection_name]
        count = collection.delete_many(query)
        print(count.deleted_count, ' documents deleted')

    def delete_collection(self, db_name, collection_name):
        database = self.mongodb_client[db_name]
        collection = database[collection_name]
        collection.drop()

    def update_one(self, db_name, collection_name, query, new_value):
        database = self.mongodb_client[db_name]
        collection = database[collection_name]
        collection.update_one(query, {"$set": new_value})

    def update_many(self, db_name, collection_name, query, new_value):
        database = self.mongodb_client[db_name]
        collection = database[collection_name]
        count = collection.update_many(query, {"$set": new_value})
        print(count.modified_count, ' documents updated')
        