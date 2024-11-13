import os
import json
import psycopg2

from flask import Flask, flash, request, redirect, url_for, render_template, send_from_directory
from werkzeug.utils import secure_filename
from db.mongodb.mongodb import MongoDB
from db.postgresql.postgresql import PostgreSQL

UPLOAD_FOLDER = os.getenv("UPLOAD_DIRECTORY")
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}
ENV_MODE = os.getenv("ENV_MODE")
class MainApp:
    def __init__(self):
        self.app = Flask(__name__)
        self.app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
        self.configurable_routes()

    def allowed_file(self, filename):
        return '.' in filename and \
            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    def configurable_routes(self):
        @self.app.route("/")
        def hello_world():
            return "<p>Hello, World!</p>"
        
        @self.app.route('/uploads/<name>')
        def download_file(name):
            return send_from_directory(self.app.config["UPLOAD_FOLDER"], name)
        
        @self.app.route('/images', methods=['GET'])
        def show_uploaded_images():
            mongo_instance = MongoDB()
            client, database, collection = mongo_instance.get_connection("file-uploads")

            data = list(collection.find({}))
            print(data)
            print("===")

            parsed = []
            for d in data:
                img_url = url_for('download_file', name=d['file_path'])

                parsed.append({
                    "image_url": img_url
                })
            
            if ENV_MODE == "backend":
                return json.dumps({
                    "data": parsed
                })
            else:
                return render_template('view_images.html', navigation=parsed)

        # @self.app.route('/order', methods=['GET', 'POST'])
        # def create_order():
        #     if request.method == 'POST':
        #         pass
        
        @self.app.route("/upload-file", methods=['GET', 'POST'])
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
                if file and self.allowed_file(file.filename):

                    # Upload the file
                    filename = secure_filename(file.filename)
                    file_path = os.path.join(self.app.config['UPLOAD_FOLDER'], filename)
                    file.save(file_path)

                    # save image_metadata to MongoDB
                    mongo_instance = MongoDB()
                    client, database, collection = mongo_instance.get_connection("file-uploads")

                    result = collection.insert_one({
                        "file_path": filename
                    })

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
        
    def run(self, **kwargs):
        self.app.run(**kwargs)

# Running the apt apt
if __name__ == "__main__":
    main_app = MainApp()
    main_app.run(debug=True)