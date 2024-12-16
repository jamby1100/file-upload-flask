import os
import json
from decimal import Decimal
from flask import flash, request, redirect, url_for, render_template
from werkzeug.utils import secure_filename
from db.mongodb.mongodb import MongoDB
from db.postgresql.postgresql import PostgreSQL
from tasks.resize_image import resize_and_upload_image
from actions.send_kafka import send_msg_async
from actions.receive_kafka import consume_message
from helpers import Helper
from bson import ObjectId
import re

ENV_MODE = os.getenv("ENV_MODE")
class UploadFile:
    def __init__(self, app):
        self.app = app

    def upload(self):
        print("AND THE FORM IS")
        print(request.form)
        if request.method == 'POST':

            # check if the post request has the file part
            if 'file' not in request.files:
                flash('No file part')
                return redirect(request.url)
            file = request.files['file']
            
            # If the user does not select a file, the browser submits an
            # empty file without a filename.
            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            if file and Helper.allowed_file(file.filename):

                # Upload the file
                filename = secure_filename(file.filename)
                print("AND THE APP IS", self.app)
                print("AND THE APP CONFIG IS", self.app.config)
                file_path = os.path.join(self.app.config['UPLOAD_DIRECTORY'], filename)
                file.save(file_path)

                # save image_metadata to MongoDB
                collection_name = "file-uploads"
                mongo_instance = MongoDB()
                client, database, collection = mongo_instance.get_connection("file-uploads")

                result = collection.insert_one({"original_image_url": filename})
                image_mongo_id = result.inserted_id
                print("Collections in the database:")
                print(database.list_collection_names())

                # Iterate through collections and print their contents
                for collection_name in database.list_collection_names():
                    print(f"\nContents of collection '{collection_name}':")
                    for document in database[collection_name].find():
                        print(document)
                # parse objectid of mongo id
                match = re.search(r"ObjectId\('([a-f0-9]+)'\)", str(result))
                if match:
                     object_id = match.group(1)
                     print(object_id,'object_id')
                
                # # send resize path kafkha
                result =  send_msg_async({"file-path": file_path, "image_mongo_id": object_id})
                
                print ('message-sent?',result)
 
                client.close()

                stock_count=int(request.form.get('initial_stock_count'))
                # save product_data to PostgreSQL
                psql_instance = PostgreSQL()
                psql_instance.connect()

                product_id = psql_instance.create_product(
                    name=request.form.get('product_name'),
                    image_mongodb_id= "12345",
                    price=250.00,
                    stock_count=stock_count,
                    review="Sample Review"
                )
                psql_instance.close()
                
                resized_path = file_path.replace(".", "_resized.")
                new_file_path = resized_path.lstrip('/tmp/')
                
                original_img_url = url_for('download_file', name=filename)
                resized_img_url = url_for('download_file', name=new_file_path)

            if ENV_MODE == "backend":
                return {
                    "filename": filename,
                    "original_img_url": original_img_url,
                    "resized_img_url": resized_img_url
                }
            else:
                return f'''
                <!doctype html>
                <html>
                    <h1>{filename}</h1>
                    <img src="{original_img_url}" alt="Original Image"></img>
                    <h1>{new_file_path} 200x200</h1>
                    <img src="{resized_img_url}" alt="Resized Image"></img>

                </html>
                '''
            redirect()
        return render_template('upload_image.html')