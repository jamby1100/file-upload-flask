import boto3
from confluent_kafka import Producer
import os

# AWS IAM authentication for Kafka
def get_iam_authentication_credentials():
    # Get AWS credentials from IAM role or environment variables
    session = boto3.Session()
    credentials = session.get_credentials().get_frozen_credentials()

    return {
        'sasl.username': 'AWS4-HMAC-SHA256',  # Static value for AWS IAM SASL
        'sasl.password': credentials.session_token,  # Temporary session token
    }

def create_kafka_producer():
    # AWS MSK cluster settings
    kafka_broker = os.getenv('KAFKA_BROKER', 'boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098')
    access_key = os.getenv('ACCESS_KEY')
    secrets_key = os.getenv('SECRETS_KEY')
    # Get IAM credentials for Kafka authentication
    iam_credentials = get_iam_authentication_credentials()

    # Create the Kafka producer with IAM SASL authentication
    producer = Producer({
        'bootstrap.servers': kafka_broker,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'AWS_MSK_IAM',
        'sasl.username': 'AWS_MSK_IAM',  # AWS IAM automatically handled
        'aws.access.key.id': access_key,  # Optional: specify AWS credentials if needed
        'aws.secret.access.key': secrets_key,  # Optional: specify AWS credentials if needed
    })

    return producer

def send_message_to_kafka(producer, topic, message):
    try:
        # Produce the message to the Kafka topic
        producer.produce(topic, value=message)
        producer.flush()
        print(f"Message sent to Kafka topic {topic}: {message}")
    except Exception as e:
        print(f"Error producing message: {e}")

# Main function to setup producer and send a test message
def main():
    # Create the Kafka producer
    producer = create_kafka_producer()
    
    # Topic name where the image processing result will be sent
    kafka_topic = 'image-processing-topic'
    
    # Message to send to Kafka (you can customize this)
    message = "Resized image uploaded and ready."

    # Send the message to Kafka
    send_message_to_kafka(producer, kafka_topic, message)

if __name__ == "__main__":
    main()
