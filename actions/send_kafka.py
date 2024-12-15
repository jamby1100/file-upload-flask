import json
import time
from time import time
from confluent_kafka import Producer
from PIL import Image

producer = Producer({
    'bootstrap.servers': 'b-3.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,b-1.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,b-2.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092',
    'socket.timeout.ms': 100,
    'api.version.request': 'false',
    'broker.version.fallback': '0.9.0',
    'message.max.bytes': 1000000000
})



# data = {"name": "xyz", "email": "xyz@"}




def send_msg_async(msg, msg_id):
    try:
        # Create the message dictionary with 'id' and 'data' as keys
        msg_json_str = {"id": msg_id, "data": json.dumps(msg)}
        
        # Accessing the 'id' and 'data' part before sending
        print("Accessing id: ", msg_json_str["id"])
        print("Accessing data: ", msg_json_str["data"])
        
        # Send the message to Kafka
        producer.produce(
            'your-topic-name',
            json.dumps(msg_json_str)  # Send the whole dictionary as a JSON string
        )
        
        print('sent-message', msg_json_str)
        producer.flush()
        return "Message sent successfully."
    except Exception as ex:
        print("Error : ", ex)
        return f"Failed to send message: {ex}"
