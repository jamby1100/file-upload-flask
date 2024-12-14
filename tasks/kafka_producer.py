from kafka import KafkaProducer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json

# AWS region where MSK cluster is located
region = 'ap-southeast-1'

# Class to provide MSK authentication token
class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        
        print(token,'tokengen')
        return token

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
    sasl_oauth_token_provider='aHR0cHM6Ly9rYWZrYS5hcC1zb3V0aGVhc3QtMS5hbWF6b25hd3MuY29tLz9BY3Rpb249a2Fma2EtY2x1c3RlciUzQUNvbm5lY3QmWC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBNFhONE9STEJBS1BRWlpGNyUyRjIwMjQxMjE0JTJGYXAtc291dGhlYXN0LTElMkZrYWZrYS1jbHVzdGVyJTJGYXdzNF9yZXF1ZXN0JlgtQW16LURhdGU9MjAyNDEyMTRUMDg0MDA1WiZYLUFtei1FeHBpcmVzPTkwMCZYLUFtei1TaWduZWRIZWFkZXJzPWhvc3QmWC1BbXotU2lnbmF0dXJlPTJiYTcwZmMxZjJmYjA1YWNjNWE4YWI0YjEyNjQxYzQzNDlkMjcyZmNkYTdkMjQxMDY2ZmFkMTc4NDkwMGFhYzYmVXNlci1BZ2VudD1hd3MtbXNrLWlhbS1zYXNsLXNpZ25lci1weXRob24lMkYxLjAuMQ',
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
