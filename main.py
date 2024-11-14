import os

from flask import Flask, flash, request, redirect, url_for, render_template, send_from_directory
from werkzeug.utils import secure_filename
from actions.upload_file import UploadFile
from actions.view_images import ViewImage
from core.config import Config
from core.celery_app import celery
class MainApp:
    def __init__(self, config_class=Config):
        self.app = Flask(__name__)
        self.app.config.from_object(config_class)

        # Initialize components
        self.upload_instance = UploadFile(self.app)
        self.images_instance = ViewImage(self.app)
        
        
        self.configurable_routes()

    def configurable_routes(self):
        @self.app.route("/")
        def hello_world():
            return "<p>Hello, World!</p>"
        
        @self.app.route('/uploads/<name>')
        def download_file(name):
            return send_from_directory(self.app.config["UPLOAD_DIRECTORY"], name)
        
        @self.app.route('/images', methods=['GET'])
        def show_uploaded_images():
            return self.images_instance.list()

        # @self.app.route('/order', methods=['GET', 'POST'])
        # def create_order():
        #     if request.method == 'POST':
        #         pass
        
        @self.app.route("/upload-file", methods=['GET', 'POST'])
        def upload_file_route():
            return self.upload_instance.upload()
        
        @self.app.route("/process-task", methods=['POST'])
        def process_task():
            # Assuming we're passing some data for the task
            task = celery.send_task('app.tasks.example_task', args=['some data'])
            return f'Task {task.id} started!'
        
        
    def run(self, **kwargs):
        self.app.run(**kwargs)

# Running the apt apt
if __name__ == "__main__":
    main_app = MainApp()
    main_app.run(debug=True)
