from confluent_kafka import Producer
from kafka import KafkaProducer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json

region= 'ap-southeast-1'
class MSKTokenProvider():
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        return token

tp = MSKTokenProvider()

producer = KafkaProducer(
    bootstrap_servers='boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retry_backoff_ms=500,
    request_timeout_ms=20000,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp,)
    # def __init__(self, bootstrap_servers, topic):
    #     self.bootstrap_servers = bootstrap_servers
    #     self.topic = topic
    #     self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})

def on_send_success(self, record_metadata):
    print(f"Message delivered to {record_metadata.topic} "
            f"[{record_metadata.partition}] at offset {record_metadata.offset}")

def on_send_error(self, err):
    print(f"Error while sending message: {err}")

def produce_resize_task(self, file_path, width, height):
    message = {
        'file_path': file_path,
        'width': width,
        'height': height
    }
    self.producer.produce(self.topic, json.dumps(message), callback=self.on_send_success)
    self.producer.flush()  # Ensure message is sent

# Continuously generate and send data
while True:
    data = produce_resize_task()
    print(data)
    try:
        future = producer.send('resize-image-topic', value=data)
        producer.flush()
        record_metadata = future.get(timeout=10)
        
    except Exception as e:
        print(e.with_traceback())