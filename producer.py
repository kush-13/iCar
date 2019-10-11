import sys
import os
import json
from confluent_kafka import Producer
import keyboard


conf = {
    'bootstrap.servers': 'omnibus-01.srvs.cloudkafka.com:9094,omnibus-02.srvs.cloudkafka.com:9094,omnibus-03.srvs.cloudkafka.com:9094',
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'},
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': 'wurdvdfa',
    'sasl.password': 'tXchfG1MeEi6LBiEX4dZtBTEaPJtDgHc'
}


prev_key = None
topic = ['wurdvdfa-testing'][0]
p = Producer(**conf)

def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d]\n' %
                            (msg.topic(), msg.partition()))

def callback(event):
    global prev_key
    if event.name in ['w','a','s','d']:
        if event.event_type == 'up' and event.name == prev_key:
            print(event.name," released !!!!")
            prev_key = 'stop'
            try:
                p.produce(topic, json.dumps({prev_key : 1}, "utf-8"), callback=delivery_callback)
            except BufferError :
                sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                                len(p))
            p.poll(0)
        elif  event.event_type == 'down' and event.name != prev_key:
            print(event.name,"  pressed !!!!")
            prev_key = event.name
            try:
                p.produce(topic, json.dumps({prev_key : 1}, "utf-8"), callback=delivery_callback)
            except BufferError :
                sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                                len(p))
            p.poll(0)
    elif event.name == 'esc':
        keyboard.unhook_all()
        print("EXIT")
        exit()
if __name__ == '__main__':
    keyboard.hook(callback,suppress=False)
    
