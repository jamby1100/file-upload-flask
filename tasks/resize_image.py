from PIL import Image
from core.celery_app import celery

@celery.task
def resize_and_upload_image(file_obj, width, height):
    file_path = file_obj["file_path"]
    file_name = file_obj["file_name"]

    resized_path = file_path.replace(".", "_resized.")
    resized_filename = file_name.replace(".", "_resized.")

    try:
        with Image.open(file_path) as img:
            img = img.resize((width, height))
            img.save(resized_path)
        final_path = resized_path.lstrip('/home/ec2-user/efs')
        return resized_filename

    except Exception as e:
        raise Exception(f"Image resizing failed: {e}")