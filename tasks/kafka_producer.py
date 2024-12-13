import os
import json
from confluent_kafka import Producer

class KafkaProducer:
    def __init__(self, topic):
        # Fetch bootstrap servers from environment variables or use default
        self.bootstrap_servers = os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS',
            'b-1-public.democluster1.gp4ygf.c3.kafka.ap-southeast-1.amazonaws.com:9198,' 
            'b-2-public.democluster1.gp4ygf.c3.kafka.ap-southeast-1.amazonaws.com:9198,' 
            'b-3-public.democluster1.gp4ygf.c3.kafka.ap-southeast-1.amazonaws.com:9198'
        )
        self.topic = topic
        
        # Initialize the Kafka producer
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})

    def on_send_success(self, err, msg):
        if err:
            print(f"Error while sending message: {err}")
        else:
            print(f"Message delivered to {msg.topic()} "
                  f"[{msg.partition()}] at offset {msg.offset()}")

    def produce_resize_task(self, file_path, width, height):
        # Create the message payload
        message = {
            'file_path': file_path,
            'width': width,
            'height': height
        }
        try:
            # Send message with callback
            self.producer.produce(self.topic, json.dumps(message), callback=self.on_send_success)
            self.producer.flush()  # Ensure message is sent
        except Exception as e:
            print(f"Failed to send message: {e}")

# Example usage
if __name__ == "__main__":
    topic = "resize-image-topic"
    producer = KafkaProducer(topic)
    producer.produce_resize_task("/path/to/image.jpg", width=200, height=200)
