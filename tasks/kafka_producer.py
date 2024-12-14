from kafka import KafkaProducer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json
import time

# AWS region where MSK cluster is located
region = 'ap-southeast-1'

# Class to provide MSK authentication token
class MSKTokenProvider:
    def __init__(self):
        self.token = None
        self.token_expiration = None
        self.region = region
        
    def token(self):
        # Check if the token is still valid
        if self.token is None or time.time() > self.token_expiration:
            # Token is either expired or not available, generate a new one
            token, expiration = MSKAuthTokenProvider.generate_auth_token(self.region)
            self.token = token
            self.token_expiration = expiration
            print(f"Generated new token: {self.token}")
        else:
            print(f"Using cached token: {self.token}")
        return self.token

# Create an instance of MSKTokenProvider class
tp = MSKTokenProvider()

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retry_backoff_ms=500,
    request_timeout_ms=30000,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp,  # Pass the instance itself
)

# Function to send resize task to Kafka
def send_resize_task(file_path, width, height):
    # Construct the task payload
    task = {
        "file_path": file_path,
        "width": width,
        "height": height
    }
    
    # Send the task to Kafka
    producer.send('image-resize', task)
    producer.flush()  # Ensure the message is sent
    print(f"Resize task sent: {task}")
