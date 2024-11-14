import os

from decimal import Decimal
from flask import flash, request, redirect, url_for, render_template
from werkzeug.utils import secure_filename
from db.mongodb.mongodb import MongoDB
from db.postgresql.postgresql import PostgreSQL

from helpers import Helper

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
                file_path = os.path.join(self.app.config['UPLOAD_FOLDER'], filename)
                file.save(file_path)

                # save image_metadata to MongoDB
                mongo_instance = MongoDB()
                client, database, collection = mongo_instance.get_connection("file-uploads")

                result = collection.insert_one({
                    "file_path": filename
                })

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
                ).close()

                img_url = url_for('download_file', name=filename)

                if ENV_MODE == "backend":
                    return {
                        "filename": filename,
                        "img_url": img_url
                    }
                else:
                    return f'''
                    <!doctype html>
                    <html>
                        <h1>{filename}</h1>
                        <img src={img_url}></img>
                    </html>
                    '''
                
                
                redirect()
        
        return render_template('upload_image.html')