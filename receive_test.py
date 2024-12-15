from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'b-3.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,'
                         'b-1.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,'
                         'b-2.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092',
    'socket.timeout.ms': 100,
      'fetch.wait.max.ms':90,
    'api.version.request': 'false',
    'broker.version.fallback': '0.9.0',
    'message.max.bytes': 1000000000,
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest',
    'session.timeout.ms': 10000,  # Default is usually 10000ms (10 seconds)
    'max.poll.interval.ms': 300000  # Must be >= session.timeout.ms
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
