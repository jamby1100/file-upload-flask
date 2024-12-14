import time
from kafka.admin import KafkaAdminClient, NewTopic
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# AWS region where MSK cluster is located
region = 'ap-southeast-1'

# Class to provide MSK authentication token
class MSKTokenProvider:
    def __init__(self):
        self.token = None
        self.token_expiry = 0

    def token(self):
        # Cache token and refresh only when expired
        current_time = time.time()
        if self.token is None or current_time >= self.token_expiry:
            self.token, expiry_time = MSKAuthTokenProvider.generate_auth_token(region)
            self.token_expiry = current_time + 900  # Tokens are valid for 15 minutes
            print("Generated new token:", self.token)
        return self.token

# Create an instance of MSKTokenProvider class
tp = MSKTokenProvider()

# Kafka Admin Client configuration
try:
    admin_client = KafkaAdminClient(
        bootstrap_servers='boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098',
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=tp,
        client_id='client1',
        api_version=(2, 8, 0),
        retry_backoff_ms=500,
        request_timeout_ms=30000,
    )

    # Topic creation
    topic_name = "mytopic"
    existing_topics = admin_client.list_topics()

    if topic_name not in existing_topics:
        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(topic_list)
        print(f"Topic '{topic_name}' has been created.")
    else:
        print(f"Topic '{topic_name}' already exists! List of topics:", existing_topics)

except Exception as e:
    print(f"Error during Kafka operation: {e}")
