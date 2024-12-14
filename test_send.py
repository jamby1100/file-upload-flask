import json
import time
from time import time
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'b-3.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,b-1.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092,b-2.unauth.xbyahs.c3.kafka.ap-southeast-1.amazonaws.com:9092',
    'socket.timeout.ms': 100,
    'api.version.request': 'false',
    'broker.version.fallback': '0.9.0',
    'message.max.bytes': 1000000000
})



data = {"name": "xyz", "email": "xyz@"}




def send_msg_async(msg):
    # print("Sending message")
    try:
        msg_json_str = str({"data": json.dumps(msg)})
        producer.produce(
            'your-topic-name',
            msg_json_str
        )
        print('sent-message', msg_json_str)
        producer.flush()
    except Exception as ex:
        print("Error : ", ex)
        
send_msg_async(data)