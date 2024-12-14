from confluent_kafka import AdminClient, NewTopic
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# MSK authentication token provider class
class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('ap-southeast-1')
        return token

# Create an instance of MSKTokenProvider class
tp = MSKTokenProvider()

# Kafka Admin Client Configuration
admin_client = AdminClient({
    'bootstrap.servers': 'boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'OAUTHBEARER',
    'sasl.oauthbearer.token': tp.token()
})

# Create a topic
new_topic = NewTopic("your-topic-name", num_partitions=3, replication_factor=2)
fs = admin_client.create_topics([new_topic])

# Check if topic creation was successful
for topic, f in fs.items():
    try:
        f.result()  # Will raise an exception if topic creation failed
        print(f"Topic {topic} created successfully")
    except Exception as e:
        print(f"Failed to create topic {topic}: {e}")
