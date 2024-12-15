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



consumer.subscribe(['your-topic-name'])

msg = consumer.poll(5)

    
print('Received message: {}'.format(msg.value().decode('utf-8')))

consumer.close()