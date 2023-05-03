from kafka import KafkaProducer
import requests
import time
import json

API_KEY = ""
API_URL = "https://api.stackexchange.com/2.3/questions?order=desc&sort=activity&site=stackoverflow"
    
producer = KafkaProducer(bootstrap_servers=['192.168.0.22:9092'])
# Continuously fetch and publish questions to Kafka
while True:
    response = requests.get(API_URL, headers={"Authorization": "Bearer " + API_KEY})
    questions = response.json()["items"]
    # Publish each question to Kafka
    for question in questions:
        print(question)
        print("\n")

        producer.send("stackoverflow_data", json.dumps(question).encode('utf-8'))
        #Wait for 3 seconds before fetching more data
    time.sleep(3)
