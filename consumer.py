from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

consumer = KafkaConsumer(
    'stackoverflow_data',
    bootstrap_servers=['192.168.0.22:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group')

es = Elasticsearch(['http://localhost:9200'])

for message in consumer:
    question = json.loads(message.value.decode('utf-8'))
    res = es.index(index="stackoverflow_questions", body=question)
    print(res['result'])
