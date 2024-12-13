import os
from flask import flash, request, redirect, url_for, render_template
from werkzeug.utils import secure_filename
from db.mongodb.mongodb import MongoDB
from db.postgresql.postgresql import PostgreSQL
from tasks.kafka_producer import KafkaProducer
from helpers import Helper
from bson import ObjectId

# Load environment variables
ENV_MODE = os.getenv("ENV_MODE", "frontend")  # Default to "frontend"

class UploadFile:
    def __init__(self, app):
        self.app = app
        self.upload_dir = self.app.config.get('UPLOAD_DIRECTORY', 'uploads')

    def upload(self):
        if request.method == 'POST':
            # Validate file in the request
            file = request.files.get('file')
            if not file or file.filename == '':
                flash('No file selected or file is missing.')
                return redirect(request.url)
            
            if not Helper.allowed_file(file.filename):
                flash('Invalid file type.')
                return redirect(request.url)

            try:
                # Secure the file name and save the file
                filename = secure_filename(file.filename)
                file_path = os.path.join(self.upload_dir, filename)
                file.save(file_path)

                # Save image metadata to MongoDB
                mongo_instance = MongoDB()
                client, database, collection = mongo_instance.get_connection("file-uploads")
                result = collection.insert_one({"original_image_url": filename})
                image_mongo_id = result.inserted_id

                # Send resize task to Kafka
                kafka_producer = KafkaProducer(
                    bootstrap_servers=os.getenv('KAFKA_BROKERS'),
                    topic='resize-image-topic'
                )
                kafka_producer.produce_resize_task(file_path, width=200, height=200)

                # Update MongoDB with placeholder resized image URL
                collection.update_one(
                    {"_id": ObjectId(image_mongo_id)},
                    {"$set": {"resized_image_url": "processing..."}}
                )
                client.close()

                # Save product metadata to PostgreSQL
                stock_count = int(request.form.get('initial_stock_count', 0))
                psql_instance = PostgreSQL()
                psql_instance.connect()
                product_id = psql_instance.create_product(
                    name=request.form.get('product_name'),
                    image_mongodb_id=str(image_mongo_id),
                    price=Decimal(request.form.get('price', '0.00')),
                    stock_count=stock_count,
                    review=request.form.get('review', 'No review')
                )
                psql_instance.close()

                # Generate URLs for frontend or API response
                original_img_url = url_for('download_file', name=filename, _external=True)
                resized_img_url = url_for('download_file', name="processing...", _external=True)

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
                        <h1>{resized_img_url} 200x200</h1>
                        <img src="{resized_img_url}" alt="Resized Image"></img>
                    </html>
                    '''
            except Exception as e:
                # Handle and log errors
                flash(f"An error occurred: {str(e)}")
                return redirect(request.url)

        # Render the upload page if GET request
        return render_template('upload_image.html')
