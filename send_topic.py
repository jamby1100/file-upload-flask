from kafka import KafkaProducer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json

# AWS region where MSK cluster is located
region = 'ap-southeast-1'

# Class to provide MSK authentication token
class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        
        print(token, 'token generated')
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
    sasl_oauth_token_provider=tp,
    api_version=(2, 8, 0),
    client_id="my.kafka.client.unique.id",
        
)

# Function to send a simple message to Kafka
def send_simple_message():
    # Construct a simple message
    message = 'hello'
    someDict = {'message':'hello'}
    kafkaValue = json.dumps(someDict, separators=(',', ':'))
    producer.send(
    topic="your-topic-name", # <-- just string
    key=bytes("00909", encoding='utf8'), # <-- encoding='utf8'
    value=bytes(kafkaValue, encoding='utf8')) # <-- bytes
    # Send the message to Kafka
    # producer.send('your-topic-name', message.encode())
    producer.flush()  # Ensure the message is sent
    print(f"Simple message sent: {message}")

# Call the function to send a simple message
send_simple_message()
