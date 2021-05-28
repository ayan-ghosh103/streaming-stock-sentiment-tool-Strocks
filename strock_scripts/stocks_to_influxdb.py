import sys
from kafka import KafkaConsumer
#from confluent_kafka import Consumer
import json
import time
import requests
from influxdb import InfluxDBClient
import rfc3339
import datetime


stk_consumer = KafkaConsumer('stocks_demo_proc', bootstrap_servers="localhost:9092")


client = InfluxDBClient(host='localhost', port=8086)
client.drop_database('influx-stocks')
client.create_database('influx-stocks')
print(client.get_list_database())

c = 0
try:
    for message in stk_consumer:
        values = json.loads(message.value.decode())

        print("Price: " + values['Price'])
        print("Volume: " + values['Volume'])
        print("--------------------------------------------------")


# TO INFLUXDB

        stk_json_body = [{
                "measurement": "stocks",
                "time": rfc3339.rfc3339(datetime.datetime.now()),
                "fields": {
                    "price": float(values['Price']),
                     "volume": values['Volume'],
                     }}]
    
        client.write_points(points=stk_json_body, database='influx-stocks')

except KeyboardInterrupt:
    sys.exit()