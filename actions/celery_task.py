from PIL import Image
from core.celery_app import celery
from db.mongodb.mongodb import MongoDB

@celery.task
def resize_and_upload_image(file_path, width, height):
    resized_path = file_path.replace(".", "_resized.")
    try:
        with Image.open(file_path) as img:
            img = img.resize((width, height))
            img.save(resized_path)

        return resized_path.lstrip('/tmp/')

    except Exception as e:
        raise Exception(f"Image resizing failed: {e}")