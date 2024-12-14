from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# Define the AWS region and Kafka configuration
region = 'ap-southeast-1'
bootstrap_servers = 'boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098'

# Token provider class for Kafka 2.6.1
class MSKTokenProvider:
    def token(self):
        try:
            # Generate an IAM authentication token
            token, _ = MSKAuthTokenProvider.generate_auth_token(region)
            print("Successfully generated IAM token for Kafka.")  # Debugging
            return token
        except Exception as e:
            print(f"Error generating token: {e}")
            raise


# Instance of the token provider
token_provider = MSKTokenProvider()

try:
    # Initialize Kafka Admin Client
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        security_protocol='SASL_SSL',
        sasl_mechanism='OAUTHBEARER',
        sasl_oauth_token_provider=token_provider,
        client_id='client1',
        api_version=(2, 6, 1), 
        request_timeout_ms=30000,
        retry_backoff_ms=500,
    )

    # Define the topic details
    topic_name = "mytopic"
    new_topic = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]

    # Check if the topic already exists
    existing_topics = admin_client.list_topics()

    if topic_name not in existing_topics:
        admin_client.create_topics(new_topics=new_topic)
        print(f"Topic '{topic_name}' created successfully.")
    else:
        print(f"Topic '{topic_name}' already exists.")

except Exception as e:
    print(f"Error during Kafka operation: {e}")
