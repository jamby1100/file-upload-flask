from confluent_kafka import Consumer, KafkaException
from PIL import Image
import json
import os

# Kafka configuration
conf = {
    'bootstrap.servers': 'boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098',  # Replace with your Kafka broker address
    'group.id': 'image-resize-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

def resize_image(file_path, width, height):
    resized_path = file_path.replace(".", "_resized.")
    try:
        with Image.open(file_path) as img:
            img = img.resize((width, height))
            img.save(resized_path)
        final_path = resized_path.lstrip('/tmp/')
        return final_path
    except Exception as e:
        print(f"Error resizing image: {e}")
        return None

def consume_resize_tasks():
    consumer.subscribe(['image-resize'])  # Subscribe to the topic

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.partition} {msg.offset}")
                else:
                    raise KafkaException(msg.error())
            else:
                task = json.loads(msg.value().decode('utf-8'))
                file_path = task['file_path']
                width = task['width']
                height = task['height']
                print(f"Received task to resize image: {file_path}, {width}x{height}")
                result = resize_image(file_path, width, height)
                if result:
                    print(f"Resized image saved to {result}")
                else:
                    print(f"Failed to resize image: {file_path}")

    except KeyboardInterrupt:
        print("Terminating consumer...")
    finally:
        consumer.close()
