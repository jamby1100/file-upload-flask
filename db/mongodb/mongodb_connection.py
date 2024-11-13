from flask import current_app as app
from pymongo import MongoClient

def create_mongodb_raw_connect():
    db_name = app.config["MONGODB_DB_NAME"]
    uri = app.config["MONGODB_DB_CONNECTION_URI"]

    try:
        client = MongoClient(uri)
        return client
    except Exception as e:
        raise Exception("The following error occurred: ", e)

def create_mongodb_connection(collection_name):
    db_name = app.config["MONGODB_DB_NAME"]
    uri = app.config["MONGODB_DB_CONNECTION_URI"]

    try:
        client = MongoClient(uri)
        database = client[db_name]
        collection = database[collection_name]
        return client, database, collection
    except Exception as e:
        raise Exception("The following error occurred: ", e)
