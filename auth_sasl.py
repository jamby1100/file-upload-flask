from kafka import KafkaProducer
from kafka.errors import KafkaError
import socket
import time
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

class MSKTokenProvider():
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('ap-southeast-1')
        return token

tp = MSKTokenProvider()

producer = KafkaProducer(
    bootstrap_servers='boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098',
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp,
    client_id=socket.gethostname(),
    api_version=(2, 8, 0),
)

topic = "your-topic-name"
# while True:
try:
    inp=input(">")
    producer.send(topic, inp.encode())
    producer.flush()
    print("Produced!")
except Exception:
    print("Failed to send message:")

producer.close()