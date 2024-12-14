from kafka.admin import KafkaAdminClient, NewTopic
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# AWS region where MSK cluster is located
region= 'ap-southeast-1'

# Class to provide MSK authentication token
class MSKTokenProvider():
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        return token

# Create an instance of MSKTokenProvider class
tp = MSKTokenProvider()

# Initialize KafkaAdminClient with required configurations
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
# create topic
topic_name="mytopic"
topic_list =[NewTopic(name=topic_name, num_partitions=1, replication_factor=2)]
existing_topics = admin_client.list_topics()
if(topic_name not in existing_topics):
    admin_client.create_topics(topic_list)
    print("Topic has been created")
else:
    print("topic already exists!. List of topics are:" + str(existing_topics))