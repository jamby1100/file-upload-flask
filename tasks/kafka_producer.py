from kafka import KafkaProducer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json

# AWS region where MSK cluster is located
region = 'ap-southeast-1'

# Class to provide MSK authentication token
class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        
        print(token,'token generated')
        return token

# Create an instance of MSKTokenProvider class
tp = MSKTokenProvider()
print(tp,'tpp')
# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retry_backoff_ms=500,
    request_timeout_ms=30000,
    security_protocol='PLAINTEXT',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp,
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
