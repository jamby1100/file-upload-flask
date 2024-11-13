from flask import current_app as app
from db.mongodb.mongodb_connection import create_mongodb_raw_connect

def create_collection():
    client = create_mongodb_raw_connect()
    db_name = app.config["MONGODB_DB_NAME"]
    database = client[db_name]
    database.create_collection("file-uploads")
    client.close()
