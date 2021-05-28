import sys
from kafka import KafkaConsumer
import json
import time
import requests
from influxdb import InfluxDBClient
import rfc3339
import datetime


consumer = KafkaConsumer('twitter_demo_proc', bootstrap_servers="localhost:9092")


client = InfluxDBClient(host='localhost', port=8086)
client.drop_database('tweets-TSLA')
client.create_database('tweets-TSLA')
print(client.get_list_database())

c = 0
try:
    for message in consumer:
        values = json.loads(message.value.decode())

        print("Tweet: " + values['Tweet'])
        print("Negative: " + values['Negative'])
        print("Positive: " + values['Positive'])
        print("Neutral: " + values['Neutral'])
        print("--------------------------------------------------")


# To InfluxDB
        json_body = [{
                "measurement": "tweets",
                "time": rfc3339.rfc3339(datetime.datetime.now()),
                "fields": {
                    "tweet": values['Tweet'],
                     "negative": float(values['Negative']),
                     "positive": float(values['Positive']),
                     "neutral": float(values['Neutral']),
                     "overallsentiment": float(values['OverallSentiment']),
                     }}]
    
        client.write_points(points=json_body, database='tweets-TSLA')


except KeyboardInterrupt:
    sys.exit()