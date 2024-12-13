from confluent_kafka import Producer
import json

class KafkaProducer:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})

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
