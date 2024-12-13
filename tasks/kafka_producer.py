from confluent_kafka import Consumer, KafkaException, KafkaError
from PIL import Image
import json
import os
import logging

# Kafka configuration
conf = {
    'bootstrap.servers': 'boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098',
    'group.id': 'image-resize-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def resize_image(file_path, width, height):
    resized_path = file_path.replace(".", "_resized.")
    try:
        with Image.open(file_path) as img:
            img = img.resize((width, height))
            img.save(resized_path)
        final_path = resized_path.lstrip('/tmp/')
        return final_path
    except Exception as e:
        logger.error(f"Error resizing image {file_path}: {e}")
        return None

def consume_resize_tasks():
    consumer.subscribe(['image-resize'])  # Subscribe to the topic

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # You can increase this timeout if needed
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"End of partition reached {msg.partition} {msg.offset}")
                else:
                    raise KafkaException(msg.error())
            else:
                task = json.loads(msg.value().decode('utf-8'))
                file_path = task['file_path']
                width = task['width']
                height = task['height']
                logger.info(f"Received task to resize image: {file_path}, {width}x{height}")
                result = resize_image(file_path, width, height)
                if result:
                    logger.info(f"Resized image saved to {result}")
                else:
                    logger.error(f"Failed to resize image: {file_path}")

                # Manually commit the offset after successful processing
                consumer.commit(msg)

    except KeyboardInterrupt:
        logger.info("Terminating consumer...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_resize_tasks()
