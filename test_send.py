import json
import time
from time import time
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'boot-i0fqmu70.c1.kafka-serverless.ap-southeast-1.amazonaws.com:9098',
    'socket.timeout.ms': 100,
    'api.version.request': 'false',
    'broker.version.fallback': '0.9.0',
    'message.max.bytes': 1000000000
})



data = {"name": "xyz", "email": "xyz@"}




def send_msg_async(msg):
    print("Sending message")
    try:
        msg_json_str = str({"data": json.dumps(msg)})
        producer.produce(
            'your-topic-name',
            msg_json_str
        )
        producer.flush()
    except Exception as ex:
        print("Error : ", ex)
        
send_msg_async(data)