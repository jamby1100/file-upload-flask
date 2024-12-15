import json
from confluent_kafka import Consumer

# Kafka Consumer configuration
consumer = Consumer({
    'bootstrap.servers': 'b-3.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,b-1.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,b-2.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092',
    'socket.timeout.ms': 1500,  # Increased to resolve blocking issues
    'fetch.wait.max.ms': 500,  # Default value; should work with adjusted socket timeout
    'api.version.request': 'false',  # Disable API version request for older broker versions
    'broker.version.fallback': '0.9.0',  # Set to match your broker version
    'message.max.bytes': 1000000000,  # Matches your current setting
    'group.id': 'mygroup',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start consuming from the earliest offset
    'enable.auto.commit': 'true',  # Optional: enable auto commit of offsets
})

# Subscribe to the Kafka topic
consumer.subscribe(['your-topic-name'])

try:
    # Poll for messages
    msg = consumer.poll(5.0)  # Wait up to 5 seconds for a message

    if msg is None:
        print("No message received")
    elif msg.error():
        print(f"Consumer error: {msg.error()}")
    else:
        # Successfully received a message
        print(f"Received message: {msg.value().decode('utf-8')}")
finally:
    # Close the consumer to release resources
    consumer.close()
