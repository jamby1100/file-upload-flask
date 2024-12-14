import os

from decimal import Decimal
from flask import flash, request, redirect, url_for, render_template
from werkzeug.utils import secure_filename
from db.mongodb.mongodb import MongoDB
from db.postgresql.postgresql import PostgreSQL
from tasks.resize_image import resize_and_upload_image
from tasks.kafka_producer import send_resize_task
from kafka_producer import KafkaProducer
from helpers import Helper
from bson import ObjectId

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
                mongo_instance = MongoDB()
                client, database, collection = mongo_instance.get_connection("file-uploads")

                result = collection.insert_one({"original_image_url": filename})
                image_mongo_id = result.inserted_id

                # Trigger Celery task and wait for the resized image URL
                # task = resize_and_upload_image.delay(file_path, width=200, height=200)
                task = send_resize_task(file_path, width=200, height=200)
                resized_path = task.get()  # Wait synchronously for the task to complete

                # Update MongoDB document with resized image URL
                collection.update_one(
                    {"_id": ObjectId(image_mongo_id)},
                    {"$set": {"resized_image_url": resized_path}}
                )

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
                
                original_img_url = url_for('download_file', name=filename)
                resized_img_url = url_for('download_file', name=resized_path)

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
                    <h1>{resized_path} 200x200</h1>
                    <img src="{resized_img_url}" alt="Resized Image"></img>
                </html>
                '''
            
            redirect()
        return render_template('upload_image.html')