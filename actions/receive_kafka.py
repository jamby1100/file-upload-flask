from confluent_kafka import Consumer

def consume_message(topic_name='your-topic-name', timeout=5.0):
    consumer = Consumer({
        'bootstrap.servers': 'b-3.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,' 
                             'b-1.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,' 
                             'b-2.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'latest',
        'socket.timeout.ms': 1000,     
        'fetch.wait.max.ms': 500,        
        'session.timeout.ms': 10000,     
        'api.version.request': 'false',  
        'broker.version.fallback': '0.9.0',  
        'message.max.bytes': 1000000000,  
    })

    consumer.subscribe([topic_name])

    try:
        msg = consumer.poll(timeout)
        if msg is None:
            print("No message received")
            return "No message received"
        elif msg.error():
            print(f"Consumer error: {msg.error()}")
            return f"Consumer error: {msg.error()}"
        else:
            message_value = msg.value().decode('utf-8')
            print(f"Received message: {message_value}")
            return message_value
    finally:
        consumer.close()

# Example usage
# message = consume_message("your-topic-name", timeout=5.0)
# print(f"Returned message: {message}")
