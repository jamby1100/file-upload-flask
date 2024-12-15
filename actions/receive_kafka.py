from confluent_kafka import Consumer
from PIL import Image
import json 

def consume_message(self,topic_name='your-topic-name', timeout=5.0):
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
            parsed_data = json.loads(message_value)
            file_path = parsed_data["file-path"]
            print(f"Received message: json.loads(input_data){message_value}")
            print(file_path,'receive path to process')
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
