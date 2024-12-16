from pymongo import MongoClient
import os

def create_mongodb_raw_connect():
    db_name = os.getenv("MONGODB_DB_NAME")
    uri = os.getenv("MONGODB_DB_CONNECTION_URI")

    print("Creating connection with: ", uri)

    try:
        client = MongoClient(uri)
    except Exception as e:
        raise Exception(
            "The following error occurred: ", e)

    return client

def create_mongodb_connection(collection_name):
    db_name = os.getenv("MONGODB_DB_NAME")
    uri = os.getenv("MONGODB_DB_CONNECTION_URI")

    try:
        client = MongoClient(uri)
        database = client[db_name]
        collection = database[collection_name]
        # start example code here
        # end example code here
        
    except Exception as e:
        raise Exception(
            "The following error occurred: ", e)

    return client, database, collection
