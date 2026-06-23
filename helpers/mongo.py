from pymongo import MongoClient, errors
import configparser
from loguru import logger

class MongoDriver:

    def __init__(self, collection: str):
        self.log = logger
        self.config = configparser.RawConfigParser()
        self.config.read("config.ini")
        try:
            uri = self.config["MongoDB"]["mongoo_uri"]
            dbname = self.config["MongoDB"]["database"]
            self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            self.db = self.client[dbname]
            self.collection = self.db[collection]

            self.client.admin.command('ping')
            self.log.info(f"Connected to MongoDB at {uri}")

        except errors.ServerSelectionTimeoutError as e:
            self.log.error(f"Error connecting to MongoDB: {e}")
        except Exception as e:
            self.log.error(f"Error connecting to MongoDB: {e}")
        
    def insert_data(self, data):
        try:
            self.collection.update_one(
                {"id": data["id"]},
                {"$set": data}, upsert=True
            )
            self.log.success(f"Data inserted into MongoDB: {data}")
        except Exception as e:
            self.log.error(f"Error inserting data into MongoDB: {e}")
    
    def search_data(self, query: dict):
        try:
            return self.collection.find_one(query)
        except Exception as e:
            self.log.error(f"Error searching data in MongoDB: {e}")
            
    def close(self):
        self.client.close()
        self.log.error("MongoDB connection closed")


