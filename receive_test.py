import json
import time
from time import time
from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'b-3.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,b-1.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,b-2.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092',
    'socket.timeout.ms': 100,
    'api.version.request': 'false',
    'broker.version.fallback': '0.9.0',
    'message.max.bytes': 1000000000,
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

def consume_message(topic_name='your-topic-name', timeout=5):
  
    consumer.subscribe([topic_name])

    try:
        msg = consumer.poll(timeout)
        if msg is None:
            print(f"No message received within {timeout} seconds.")
            return None

        if msg.error():
            print(f"Error in message: {msg.error()}")
            return None

        decoded_message = msg.value().decode('utf-8')
        print(f"Received message: {decoded_message}")
        return decoded_message

    except Exception as e:
        print(f"Error consuming message: {e}")
        return None

    finally:
        consumer.close()
        print("Consumer closed.")
