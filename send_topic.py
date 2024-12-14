from confluent_kafka import AdminClient, NewTopic
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# AWS region where MSK cluster is located
region = 'ap-southeast-1'

# Class to provide MSK authentication token
class MSKTokenProvider():
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        return token

# Create an instance of MSKTokenProvider class
tp = MSKTokenProvider()

# Initialize AdminClient with required configurations
conf = {
    'bootstrap.servers': 'boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098',  
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'AWS_MSK_IAM',  # Use AWS_MSK_IAM for IAM authentication
    'sasl.username': '',  # Leave blank as IAM is used
    'sasl.password': '',  # Leave blank as IAM is used
    'client.id': 'client1',
    'sasl.token.provider': tp  # Provide the token provider
}

admin_client = AdminClient(conf)

# Create a topic
topic_name = "mytopic"
topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=2)]

existing_topics = admin_client.list_topics()
if topic_name not in existing_topics:
    admin_client.create_topics(topic_list)
    print("Topic has been created")
else:
    print(f"Topic already exists! List of topics are: {existing_topics}")
