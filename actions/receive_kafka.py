from confluent_kafka import Consumer
from PIL import Image
import json 
from db.mongodb.mongodb import MongoDB
from bson import ObjectId

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
            cleaned_message = message_value[2:-2]
            parsed_data = json.loads(cleaned_message)
            file_path = parsed_data.get("file-path")
            image_mongo_id = parsed_data.get("image_mongo_id") 
            # parsed_data = json.loads(cleaned_message) 
            # inner_json = json.loads(parsed_data)
            # file_path = inner_json.get("file-path")
            # parsed_data = json.loads(message_value)
            print("Received message:",file_path)
            task = resize_and_upload_image(file_path,image_mongo_id, width=200, height=200)
            
            print(task,"path resize")
            return cleaned_message
    finally:
        consumer.close()

def resize_and_upload_image(file_path,image_mongo_id, width, height):
    resized_path = file_path.replace(".", "_resized.")
    try:
        with Image.open(file_path) as img:
            img = img.resize((width, height))
            img.save(resized_path)
        final_path = resized_path.lstrip('/tmp/')

        return final_path

    except Exception as e:
        raise Exception(f"Image resizing failed: {e}")
    
def update_mongodb(image_mongo_id,message_dict):
     mongo_instance = MongoDB()
     collection = mongo_instance.get_connection("file-uploads")
     
     try:
        collection.update_one(
                        {"_id": ObjectId(image_mongo_id)},
                        {"$set": {"resized_image_url": message_dict}}
                    )
        return "Updated Successfully"

     except Exception as e:
        raise Exception(f"Update Failed: {e}")
     
