import logging
from kafka import KafkaProducer, KafkaError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS region where MSK cluster is located
region = 'ap-southeast-1'

# Class to provide MSK authentication token
class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        logger.info(f"Token generated: {token[:10]}...")  # Log first 10 chars of token
        return token

# Create an instance of MSKTokenProvider class
tp = MSKTokenProvider()
logger.info(f"Token provider created: {tp}")

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retry_backoff_ms=500,
    request_timeout_ms=30000,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp,
    api_version=(2, 8, 0),
)

# Check if the producer is connected
max_retries = 5
retries = 0

while retries < max_retries:
    try:
        # Test the connection by sending a dummy message (this won't affect your actual operations)
        producer.partitions_for('image-resize')
        logger.info("Successfully connected to the Kafka broker!")
        break
    except KafkaError as e:
        retries += 1
        logger.error(f"Connection failed, retrying {retries}/{max_retries}: {e}")
        time.sleep(5)
else:
    logger.error("Failed to connect to the Kafka broker after multiple retries.")

# Function to send resize task to Kafka
def send_resize_task(file_path, width, height):
    # Construct the task payload
    task = {
        "file_path": file_path,
        "width": width,
        "height": height
    }
    
    # Send the task to Kafka
    try:
        producer.send('image-resize', task)
        producer.flush()  # Ensure the message is sent
        logger.info(f"Resize task sent: {task}")
    except KafkaError as e:
        logger.error(f"Failed to send resize task: {e}")
