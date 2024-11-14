from celery import Celery
from flask import Flask
from core.config import Config

# Initialize Flask and load configuration
def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    return app

app = create_app()

# Initialize Celery with Flask app configuration
celery = Celery(
    app.name,
    result_backend=app.config['RESULT_BACKEND'],  # Updated to match new setting
    broker=app.config['CELERY_BROKER_URL'],
    broker_connection_retry_on_startup=True,
    mongodb_backend_settings=app.config['MONGODB_BACKEND_SETTINGS'],  # Updated to match new setting
)
celery.conf.update(app.config)

class ContextTask(celery.Task):
    def __call__(self, *args, **kwargs):
        with app.app_context():
            return self.run(*args, **kwargs)

celery.Task = ContextTask