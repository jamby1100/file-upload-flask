
import os

from pymongo import MongoClient

class MongoDB:

  def __init__(self) -> None:
    self.db_name = os.getenv("MONGODB_DB_NAME")
    self.uri = os.getenv("MONGODB_DB_CONNECTION_URI")

  def get_connection(self, collection_name):
    try:
        client = MongoClient(self.uri)
        database = client[self.uri]
        collection = database[collection_name]        
    except Exception as e:
        raise Exception(
            "The following error occurred: ", e)

    return client, database, collection