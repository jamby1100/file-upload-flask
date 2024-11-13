import os

class Config:
    UPLOAD_DIRECTORY = os.getenv('UPLOAD_DIRECTORY', '/tmp')
    MONGODB_DB_CONNECTION_URI = os.getenv('MONGODB_DB_CONNECTION_URI', 'mongodb://localhost:27017/')
    MONGODB_DB_NAME = os.getenv('MONGODB_DB_NAME', 'ecv-jmp-file-upload-app')

    # PostgreSQL settings (if required for other parts of the app)
    POSTGRESQL_DB_USERNAME = os.getenv('POSTGRESQL_DB_USERNAME', 'flask_photo_app_admin')
    POSTGRESQL_DB_PASSWORD = os.getenv('POSTGRESQL_DB_PASSWORD', 'sac2c2qec1131DSq@#1')
    POSTGRESQL_DB_DATABASE_NAME = os.getenv('POSTGRESQL_DB_DATABASE_NAME', 'ecv_file_upload_app_psql')
    POSTGRESQL_DB_HOST = os.getenv('POSTGRESQL_DB_HOST', 'localhost')

    # Celery configuration
    CELERY_BROKER_URL = 'redis://localhost:6379/0'
    RESULT_BACKEND = 'mongodb://localhost:27017/'
    MONGODB_BACKEND_SETTINGS = {
        'database': MONGODB_DB_NAME,
        'taskmeta_collection': 'celery_taskmeta'
    }
