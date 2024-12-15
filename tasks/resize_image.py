from PIL import Image
from core.celery_app import celery

@celery.task
def resize_and_upload_image(file_path, width, height):
    resized_path = file_path.replace(".", "_resized.")
    try:
        with Image.open(file_path) as img:
            img = img.resize((width, height))
            img.save(resized_path)
        final_path = resized_path.lstrip('/home/ec2-user/efs')
        return final_path

    except Exception as e:
        raise Exception(f"Image resizing failed: {e}")