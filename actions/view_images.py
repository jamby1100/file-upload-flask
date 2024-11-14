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