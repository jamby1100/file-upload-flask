from kafka import KafkaProducer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json
import random
from datetime import datetime

topicname = 'resize-image-topic'  # Change the topic name to your resize image topic

BROKERS = 'boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098'
region = 'ap-southeast-1'

class MSKTokenProvider():
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        return token

# Instantiate the token provider
tp = MSKTokenProvider()

# Fetch the token from the MSKTokenProvider
oauth_token = tp.token()

# Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retry_backoff_ms=500,
    request_timeout_ms=20000,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_plain_username='',  # For OAuthBearer, username is not required
    sasl_plain_password=oauth_token  # Use the OAuth token as the password
)

# Method to generate resize task metadata
def generate_resize_metadata(file_path, width, height):
    now = datetime.now()
    event_time = now.strftime("%Y-%m-%d %H:%M:%S")
    
    resize_data = {
        "file_path": file_path,      # Path to the file that needs resizing
        "width": width,              # Desired width for the image
        "height": height,            # Desired height for the image
        "event_time": event_time,    # Timestamp of the resize task
    }
    return resize_data

# Continuously generate and send resize task data
while True:
    # Example image resize task with random file path and size
    file_path = f"path/to/image_{random.randint(1000, 9999)}.jpg"
    width = random.randint(100, 1000)  # Random width between 100 and 1000
    height = random.randint(100, 1000)  # Random height between 100 and 1000

    # Generate metadata for the resize task
    resize_metadata = generate_resize_metadata(file_path, width, height)
    
    print(resize_metadata)  # Print the metadata (you can remove this in production)
    
    try:
        # Send the metadata to the Kafka topic
        future = producer.send(topicname, value=resize_metadata)
        producer.flush()  # Ensure the message is sent
        record_metadata = future.get(timeout=10)
    except Exception as e:
        print(f"Error while sending message: {e}")
