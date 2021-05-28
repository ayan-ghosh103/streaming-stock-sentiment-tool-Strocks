import sys
import requests
import requests_oauthlib
import json
from confluent_kafka import Producer
import socket

#Set Credentials
ACCESS_TOKEN = '85800407-uqTaX6vphQFhxzFxxRfE3G5p0XpHqN2u4E2AcIVxA'
ACCESS_SECRET = 'GjdRROSXJcEmEEicfcfuZqs2iXltf7tGKQ8kGFIlUbsAu'
CONSUMER_KEY = '6IsQZ4VTokrZK70XydYVYEoUu'
CONSUMER_SECRET = 'kspOStrO458xAO3x6dGW2kbWQDPLhDJNkTi8o85sWW7tgkJiP2'

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'twitter_demo'

my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('track','TSLA')] #filtering tweets mentioning Tesla stock
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response

def repair_encoding(x):
    return x.encode('ascii', 'ignore').decode('ascii').replace('\n', ' ').replace('\t', ' ')

def send_tweets_to_kafka(http_resp, producer, topic):
    for line in http_resp.iter_lines():
        try:
            # JSON load
            full_tweet = json.loads(line)
            # Extraction
            tweet_text = repair_encoding(full_tweet['text'])
            # Print
            print("Tweet Text: " + tweet_text)
            # Send to Kafka
            producer.produce(topic, value=tweet_text)
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
            
conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}

producer = Producer(conf)
http_resp = get_tweets()
send_tweets_to_kafka(http_resp, producer, KAFKA_TOPIC)