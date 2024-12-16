from confluent_kafka import Consumer
from PIL import Image
import json 
from db.mongodb.mongodb import MongoDB
from bson import ObjectId

def consume_message( topic_name='your-topic-name', timeout=5.0):
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
            
            # Check if 'image_mongo_id' exists in the parsed data
            image_mongo_id = parsed_data.get("image_mongo_id")
            if not image_mongo_id:
                print("No image_mongo_id found in message")
                return "No image_mongo_id found in message"

            file_path = parsed_data.get("file-path")
            print("Received message:", file_path)

            # Proceed with processing if 'image_mongo_id' is found
            task = resize_and_upload_image(file_path, image_mongo_id, width=200, height=200)
            
            return {"file-path": task, "image_mongo_id": image_mongo_id}
    finally:
        consumer.close()

def consume_message_loop(topic_name='your-topic-name', timeout=5.0):
    while True:
        message = consume_message(topic_name, timeout)
        print(message,"loop result")
        if message == "No message received":
            print("No messages received, stopping.")
            break
        else:
            print(f"Processed: {message}")

def resize_and_upload_image(file_path,image_mongo_id, width, height):
    resized_path = file_path.replace(".", "_resized.")
    print("trying image resize")

    try:
        with Image.open(file_path) as img:
            img = img.resize((width, height))
            img.save(resized_path)
        
        print("resize completed")
        final_path = resized_path.lstrip('/tmp/')
        
        # update mongodb
        print("updating mongodb")
        print("updating with MONGODB ID", image_mongo_id)
        update_mongodb(image_mongo_id, final_path)
        print("updating mongodb COMPLETED")
        
        return final_path

    except Exception as e:
        raise Exception(f"Image resizing failed: {e}")
    
def update_mongodb(image_mongo_id,new_file_path):
     mongo_instance = MongoDB()
     client, database, collection = mongo_instance.get_connection("file-uploads")
     
     try:
        collection.update_one(
                        {"_id": ObjectId(image_mongo_id)},
                        {"$set": {"resized-image-path": new_file_path, "image_mongo_id": image_mongo_id}}  
                    )
        client.close()
        return "Updated Successfully"

     except Exception as e:
        raise Exception(f"Update Failed: {e}")
    
consume_message_loop()

     
