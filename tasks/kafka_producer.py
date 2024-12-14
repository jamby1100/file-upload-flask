from confluent_kafka import Consumer, KafkaException, KafkaError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from PIL import Image
import json
import os

# AWS MSK IAM Token Provider
msk_auth = MSKAuthTokenProvider()

# Kafka configuration with IAM authentication
conf = {
    'bootstrap.servers': 'boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098',
    'group.id': 'image-resize-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,  # Auto commit offsets
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'AWS_MSK_IAM',
    'sasl.username': msk_auth.get_username(),
    'sasl.password': msk_auth.get_password(),
}

consumer = Consumer(conf)

def resize_image(file_path, width, height):
    try:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return None

        base, ext = os.path.splitext(file_path)
        resized_path = f"{base}_resized{ext}"

        with Image.open(file_path) as img:
            img = img.resize((width, height))
            img.save(resized_path)

        return resized_path
    except Exception as e:
        print(f"Error resizing image: {e}")
        return None

def consume_resize_tasks():
    consumer.subscribe(['image-resize'])

    try:
        while True:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.partition} {msg.offset}")
                else:
                    raise KafkaException(msg.error())
            else:
                try:
                    task = json.loads(msg.value().decode('utf-8'))
                    file_path = task.get('file_path')
                    width = task.get('width')
                    height = task.get('height')

                    if not file_path or not width or not height:
                        print(f"Invalid task received: {task}")
                        continue

                    print(f"Received task to resize image: {file_path}, {width}x{height}")
                    result = resize_image(file_path, width, height)
                    if result:
                        print(f"Resized image saved to {result}")
                    else:
                        print(f"Failed to resize image: {file_path}")
                except json.JSONDecodeError as e:
                    print(f"Invalid JSON in message: {e}")
    except KeyboardInterrupt:
        print("Terminating consumer...")
    finally:
        consumer.close()
