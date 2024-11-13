from flask import current_app as app
from pymongo import MongoClient

class MongoDB:
    def __init__(self) -> None:
        # Access the existing Flask app configuration
        self.db_name = app.config['MONGODB_DB_NAME']
        self.uri = app.config["MONGODB_DB_CONNECTION_URI"]

    def get_connection(self, collection_name):
        try:
            client = MongoClient(self.uri)
            database = client[self.db_name]
            collection = database[collection_name]
            return client, database, collection
        except Exception as e:
            raise Exception("The following error occurred: ", e)
