import os
import json
from flask import Flask, flash, request, redirect, url_for, render_template, send_from_directory
from werkzeug.utils import secure_filename
from db.mongodb.mongodb import MongoDB
from db.postgresql.postgresql import PostgreSQL

from core.config import Config
from core.celery_app import celery
from pymongo import MongoClient
from actions.celery_task import resize_and_upload_image
from bson import ObjectId

ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}
ENV_MODE = os.getenv("ENV_MODE")

app = Flask(__name__)
app.config.from_object(Config)

# Set up MongoDB
client = MongoClient(app.config['MONGODB_DB_CONNECTION_URI'])
mongo_db = client[app.config['MONGODB_DB_NAME']]


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/upload-file', methods=['GET', 'POST'])
def upload_file():
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
        if file and allowed_file(file.filename):

            # Upload the file
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_DIRECTORY'], filename)
            file.save(file_path)

             # Save initial image metadata to MongoDB
            mongo_instance = MongoDB()
            client, database, collection = mongo_instance.get_connection("file-uploads")

            result = collection.insert_one({"original_image_url": filename})
            image_mongo_id = result.inserted_id

            # Trigger Celery task and wait for the resized image URL
            task = resize_and_upload_image.delay(file_path, width=200, height=200)
            resized_path = task.get()  # Wait synchronously for the task to complete

            # Update MongoDB document with resized image URL
            collection.update_one(
                {"_id": ObjectId(image_mongo_id)},
                {"$set": {"resized_image_url": resized_path}}
            )
            client.close()

            # save product_data to PostgreSQL
            psql_instance = PostgreSQL()
            psql_instance.connect()
            psql_instance.create_product(
                name=request.form.get('product_name'),
                image_mongodb_id= "12345",
                stock_count=int(request.form.get('initial_stock_count')),
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


@app.route('/images', methods=['GET'])
def show_uploaded_images():
    mongo_instance = MongoDB()
    client, database, collection = mongo_instance.get_connection("file-uploads")

    # Retrieve data from MongoDB
    data = list(collection.find({}))

    # Process each document to include both original and resized URLs
    parsed = []
    for d in data:
        original_img_url = url_for('download_file', name=d['original_image_url'])
        resized_img_url = url_for('download_file', name=d['resized_image_url'])

        parsed.append({
            "original_image_url": original_img_url,
            "resized_image_url": resized_img_url
        })
    
    if ENV_MODE == "backend":
        return json.dumps({
            "data": parsed
        })
    else:
        return render_template('view_images.html', navigation=parsed)


# @app.route('/order', methods=['GET', 'POST'])
# def create_order():
#     if request.method == 'POST':
#         pass

@app.route('/uploads/<name>')
def download_file(name):
    return send_from_directory(app.config["UPLOAD_DIRECTORY"], name)