from confluent_kafka import Producer
import json
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

region = 'ap-southeast-1'

class MSKTokenProvider():
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        return token

tp = MSKTokenProvider()

def on_send_success(record_metadata):
    print(f"Message delivered to {record_metadata.topic} "
          f"[{record_metadata.partition}] at offset {record_metadata.offset}")

def on_send_error(err):
    print(f"Error while sending message: {err}")

def produce_resize_task(producer, topic, file_path, width, height):
    # Here, the file_path is passed directly into the function
    message = {
        'file_path': file_path,  # The file path comes directly as an argument
        'width': width,
        'height': height
    }
    producer.produce(topic, json.dumps(message).encode('utf-8'), callback=on_send_success)
    producer.flush()

producer = Producer({
    'bootstrap.servers': 'b-1.democluster1.gp4ygf.c3.kafka.ap-southeast-1.amazonaws.com:9098,b-3.democluster1.gp4ygf.c3.kafka.ap-southeast-1.amazonaws.com:9098,b-2.democluster1.gp4ygf.c3.kafka.ap-southeast-1.amazonaws.com:9098',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'OAUTHBEARER',
    'sasl.oauth.token.provider': tp
})

# Example: Actual dynamic file path
file_path = "path/to/actual/file.jpg"  # This would be dynamically set elsewhere in your code

# Calling produce_resize_task with dynamic file path
width = 800
height = 600
produce_resize_task(producer, 'resize-image-topic', file_path, width, height)
print(f"Sent resize task for file: {file_path}")

try:
    # Sending the message to Kafka
    future = producer.send('resize-image-topic', value=json.dumps({
        'file_path': file_path,
        'width': width,
        'height': height
    }))
    producer.flush()
    record_metadata = future.get(timeout=10)
except Exception as e:
    print(f"Error while sending message: {e}")
