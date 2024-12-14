from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from PIL import Image
import json


# AWS Region where MSK cluster is located
AWS_REGION = 'ap-southeast-1'
BROKERS = 'boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098' 

# Class for providing MSK IAM authentication token
class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        return token


# Create an instance of the MSK token provider
tp = MSKTokenProvider()

# Kafka Producer Configuration
producer_config = {
    'bootstrap.servers': BROKERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'OAUTHBEARER',
    'sasl.oauth.token.provider': tp,
    'linger.ms': 10,
    'retry.backoff.ms': 500,
    'request.timeout.ms': 20000,
    'value.serializer': lambda v: json.dumps(v).encode('utf-8')
}

producer = Producer(producer_config)

# Kafka Consumer Configuration
consumer_config = {
    'bootstrap.servers': BROKERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'OAUTHBEARER',
    'sasl.oauth.token.provider': tp,
    'group.id': 'image-resize-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)


# Image Resizing Function
def resize_image(file_path, width, height):
    """
    Resizes an image to the specified dimensions.

    Args:
        file_path (str): Path to the image file.
        width (int): Desired width.
        height (int): Desired height.

    Returns:
        str: Path to the resized image if successful; otherwise None.
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


# Kafka Producer Function
def send_resize_task(file_path, width, height):
    """
    Sends a resize task to the Kafka topic.

    Args:
        file_path (str): Path to the image file.
        width (int): Desired width of the resized image.
        height (int): Desired height of the resized image.
    """
    topic_name = "image-resize"
    task = {
        "file_path": file_path,
        "width": width,
        "height": height
    }
    try:
        producer.produce(topic_name, value=json.dumps(task))
        producer.flush()
        print(f"Resize task sent: {task}")
    except Exception as e:
        print(f"Failed to send resize task: {e}")


# Kafka Consumer Function
def consume_resize_tasks():
    """
    Consumes image resize tasks from Kafka and processes them.
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


# Main Execution
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "produce":
        # Example: Send a resize task
        send_resize_task("/tmp/example.jpg", 200, 200)
    else:
        # Default: Start the consumer
        consume_resize_tasks()
