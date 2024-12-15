from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'b-3.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,b-1.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,b-2.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092',
    'socket.timeout.ms': 1500,  # Increased to avoid blocking
    'fetch.wait.max.ms': 500,  # Default value
    'api.version.request': 'false',  # Keep disabled for older brokers
    'broker.version.fallback': '0.9.0',  # Matches your broker version
    'message.max.bytes': 1000000000,  # Message size limit
    'group.id': 'mygroup',  # Consumer group ID
    'auto.offset.reset': 'earliest',  # Start consuming from earliest offset
    'session.timeout.ms': 30000,  # Session timeout (45 seconds)
    'max.poll.interval.ms': 30000,  # Lower than session.timeout.ms for compatibility
})

consumer.subscribe(['your-topic-name'])

try:
    msg = consumer.poll(5.0)
    if msg is None:
        print("No message received")
    elif msg.error():
        print(f"Consumer error: {msg.error()}")
    else:
        print(f"Received message: {msg.value().decode('utf-8')}")
finally:
    consumer.close()
