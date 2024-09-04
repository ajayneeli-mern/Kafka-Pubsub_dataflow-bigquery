from kafka import KafkaConsumer
import json
from google.cloud import pubsub_v1
import os

def is_valid_json(data):
    try:
        json.loads(data)
        return True
    except ValueError as e:
        return False

def publish_to_pubsub(topic_name, message):
    """Publishes a message to a Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('just-camera-432415-h9', topic_name)
    message_data = message.encode('utf-8')
    
    future = publisher.publish(topic_path, data=message_data)
    print(f"Published message to {topic_name}. Message ID: {future.result()}")

# Kafka topic from which messages will be consumed
topic = 'my-topic'
consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest', enable_auto_commit=True,
                         group_id='my-group', value_deserializer=lambda x: 
                         json.loads(x.decode('utf-8')))

# Pub/Sub topic name
pubsub_topic = 'streaming-df'

# Start consuming messages and publish valid ones to Pub/Sub
for message in consumer:
    json_data = json.dumps(message.value)
    if is_valid_json(json_data):
        # Publish valid JSON message to Pub/Sub
        publish_to_pubsub(pubsub_topic, json_data)
        print(f"Valid JSON message with offset {message.offset} published to Pub/Sub topic {pubsub_topic}.")
    else:
        print(f"Invalid JSON message at offset {message.offset} skipped.")

consumer.close()
