import os
import json

from flask import url_for, render_template
from db.mongodb.mongodb import MongoDB

ENV_MODE = os.getenv("ENV_MODE")
class ViewImage:
    def __init__(self, app):
        self.app = app

    def list(self):
        mongo_instance = MongoDB()
        client, database, collection = mongo_instance.get_connection("file-uploads")

        # Retrieve data from MongoDB
        data = list(collection.find({}))

        # Process each document to include both original and resized URLs
        parsed = []
        for d in data:
            original_image_url_raw = d.get('original_image_url', False)
            resized_img_url_raw = d.get('resized_image_url', False)

            print("and the raw are...")
            print(d)
            print(original_image_url_raw)
            print(resized_img_url_raw)
            
            resized_img_url = None
            original_img_url = None

            if original_image_url_raw:
                original_img_url = url_for('download_file', name=original_image_url_raw)
            
            if resized_img_url_raw:
                resized_img_url_raw = resized_img_url_raw.replace("home/ec2-user/efs/", "")
                resized_img_url = url_for('download_file', name=resized_img_url_raw)

            parsed.append({
                "original_image_url": original_img_url,
                "resized_image_url": resized_img_url,
                "_id": d["_id"]
            })
        
        if ENV_MODE == "backend":
            return json.dumps({
                "data": parsed
            })
        else:
            return render_template('view_images.html', navigation=parsed)
