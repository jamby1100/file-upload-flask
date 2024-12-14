from kafka.admin import KafkaAdminClient, NewTopic
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# AWS region where MSK cluster is located
region= '<UPDATE_AWS_REGION_NAME_HERE>'

# Class to provide MSK authentication token
class MSKTokenProvider():
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        return token

# Create an instance of MSKTokenProvider class
tp = MSKTokenProvider()

# Initialize KafkaAdminClient with required configurations
admin_client = KafkaAdminClient(
    bootstrap_servers='<UPDATE_BOOTSTRAP_SERVER_STRING_HERE>',
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp,
    client_id='client1',
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