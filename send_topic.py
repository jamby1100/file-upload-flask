from kafka.admin import KafkaAdminClient, NewTopic
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# AWS region where MSK cluster is located
region = 'ap-southeast-1'

# Class to provide MSK authentication token
class MSKTokenProvider:
    def token(self):
        # Generate a token using the MSKAuthTokenProvider
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        print(token,'genrate token')
        return token

# Create an instance of MSKTokenProvider class
tp = MSKTokenProvider()

# Kafka Admin Client configuration
try:
    admin_client = KafkaAdminClient(
        bootstrap_servers='boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098',
        security_protocol='PLAINTEXT',
        # sasl_mechanism='OAUTHBEARER',
        # sasl_oauth_token_provider=tp,
        client_id='client1',
        api_version=(2, 8, 0), 
        retry_backoff_ms=500,
        request_timeout_ms=30000,
    )

    # Topic creation
    topic_name = "mytopic"
    topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]  
    existing_topics = admin_client.list_topics()

    if topic_name not in existing_topics:
        admin_client.create_topics(topic_list)
        print("Topic has been created")
    else:
        print("Topic already exists! List of topics:", existing_topics)
except Exception as e:
    print(f"Error: {e}")
