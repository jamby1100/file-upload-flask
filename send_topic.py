from confluent_kafka import Producer
import boto3
import json

# Fetch MSK Bootstrap Servers
def get_msk_bootstrap_servers(cluster_arn):
    client = boto3.client("kafka")
    response = client.get_bootstrap_brokers(ClusterArn=cluster_arn)
    return response["BootstrapBrokerStringSaslIam"]

# Produce message to the MSK topic
def produce_message(bootstrap_servers, topic_name, message):
    # Configure Kafka Producer with IAM SASL
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'AWS_MSK_IAM',
        'sasl.username': '',  
        'sasl.password': '', 
    }

    producer = Producer(conf)

    try:
        # Send message to topic
        producer.produce(topic_name, value=json.dumps(message))
        producer.flush()  # Ensure all messages are sent
        print(f"Message sent to topic '{topic_name}': {message}")
    except Exception as e:
        print(f"Failed to produce message: {e}")

if __name__ == "__main__":
    # Replace with your MSK Cluster ARN and topic name
    MSK_CLUSTER_ARN = "arn:aws:kafka:ap-southeast-1:874957933250:cluster/imagecaching/3311efe7-9e54-4db5-b0e9-8a9820bcc9e4-s1"
    TOPIC_NAME = "test-topic"

    # Sample message
    MESSAGE = {"key": "value"}

    # Fetch bootstrap servers and produce a message
    bootstrap_servers = get_msk_bootstrap_servers(MSK_CLUSTER_ARN)
    produce_message(bootstrap_servers, TOPIC_NAME, MESSAGE)
