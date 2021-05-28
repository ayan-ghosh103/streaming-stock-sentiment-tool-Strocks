import uuid
import json
import requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer
import socket


def get_stock():
    url = 'https://ca.finance.yahoo.com/quote/TSLA?p=TSLA&.tsrc=fin-tre-srch'
    page = requests.get(url,headers={'Accept-Encoding': 'identity'})
    html = BeautifulSoup(page.text, 'html.parser')
    price = html.find('span', class_ = 'Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)').text
    volume = html.find('span',{'data-reactid':'72','class':'Trsdu(0.3s)'}).text
    new_message = {"Price":price,"Volume":volume}
    print(new_message)
    return new_message


def kafka_stock_producer():
    bootstrap_servers = "127.0.0.1:9092"
    
    topic = "stocks_demo"
    p = Producer({'bootstrap.servers': bootstrap_servers, 'client.id': socket.gethostname()})
    while 1:
        new_message = get_stock()

        record_key = str(uuid.uuid4())
        record_value = json.dumps(new_message) 

        p.produce(topic,key=record_key,value=record_value)
        p.poll(1)   
        

if __name__ == "__main__":
    kafka_stock_producer()