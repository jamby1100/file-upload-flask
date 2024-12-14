from confluent_kafka import Consumer, KafkaException, KafkaError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from PIL import Image
import json
import os


# AWS Region where the MSK cluster is located
AWS_REGION = 'ap-southeast-1'

# Kafka configuration
class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        return token


# Create an instance of the MSK token provider
token_provider = MSKTokenProvider()

consumer_config = {
    'bootstrap.servers': 'boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'OAUTHBEARER',
    'sasl.oauth.token.provider': token_provider,
    'group.id': 'image-resize-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)


def resize_image(file_path, width, height):
    """
    Resizes an image to the specified width and height.

    Args:
        file_path (str): Path to the image file.
        width (int): Desired width of the resized image.
        height (int): Desired height of the resized image.

    Returns:
        str: Path to the resized image if successful, None otherwise.
    """
    resized_path = file_path.replace(".", "_resized.")
    try:
        with Image.open(file_path) as img:
            img = img.resize((width, height))
            img.save(resized_path)
        return resized_path
    except Exception as e:
        print(f"Error resizing image: {e}")
        return None


def consume_resize_tasks():
    """
    Consumes image resize tasks from a Kafka topic and processes them.
    """
    topic_name = "image-resize"
    consumer.subscribe([topic_name])
    print(f"Subscribed to topic '{topic_name}'. Waiting for resize tasks...")

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
                # Decode the message and process the task
                try:
                    task = json.loads(msg.value().decode('utf-8'))
                    file_path = task.get('file_path')
                    width = task.get('width')
                    height = task.get('height')

                    if not file_path or not width or not height:
                        print(f"Invalid task data: {task}")
                        continue

                    print(f"Processing resize task: {task}")
                    resized_image_path = resize_image(file_path, width, height)

                    if resized_image_path:
                        print(f"Resized image saved at {resized_image_path}")
                    else:
                        print(f"Failed to resize image: {file_path}")
                except json.JSONDecodeError:
                    print(f"Failed to decode message: {msg.value()}")

    except KeyboardInterrupt:
        print("Consumer interrupted.")
    finally:
        consumer.close()


# Main entry point
if __name__ == "__main__":
    try:
        consume_resize_tasks()
    except Exception as e:
        print(f"Error occurred: {e}")
