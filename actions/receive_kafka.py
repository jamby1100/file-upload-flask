from confluent_kafka import Consumer

def consume_message(topic_name='your-topic-name', timeout=5.0):
    consumer = Consumer({
        'bootstrap.servers': 'b-3.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,' 
                             'b-1.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,' 
                             'b-2.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest',
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

def resize_and_upload_image(file_path, width, height):
    resized_path = file_path.replace(".", "_resized.")
    try:
        with Image.open(file_path) as img:
            img = img.resize((width, height))
            img.save(resized_path)
        final_path = resized_path.lstrip('/tmp/')
        return final_path

    except Exception as e:
        raise Exception(f"Image resizing failed: {e}")
# Example usage
# message = consume_message("your-topic-name", timeout=5.0)
# print(f"Returned message: {message}")
