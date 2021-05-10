#!/usr/bin/python3

import paho.mqtt.client as mqtt
from datetime import date
import datetime
import time
import os
import pymongo
from time import gmtime, strftime

mytime = str(strftime("%Y-%m-%d %H:%M:%S", gmtime()))


buffer =[]
timestamp = str(datetime.datetime.now())

print('starting the script....'+timestamp)

def writedb(message):

    #client = pymongo.MongoClient(
     
    client = pymongo.MongoClient(
        "mongodb+srv://<username>:<password>@cluster0.8qipc.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    db = client.test_database
    collection = db.test_collection
    post = {'message':message}


def on_message(client, userdata, message):
    ble_payload = str(message.payload.decode("utf-8"))
    print("received message: " ,ble_payload)
    f = open(timestamp+'.csv', 'a')
    f.write(str(datetime.datetime.now()))
    f.write(ble_payload)
    f.write('\n')
    #timestamp = str(datetime.datetime.now())
    #print(mytime)
    #print('timestamp ----'+str(datetime.datetime.now()))
#   writedb(ble_payload)
    buffer.append(str(message.payload.decode("utf-8")))

    f.close()

#mqttBroker ="104.36.12.156"
mqttBroker="10.220.252.10"


client = mqtt.Client("Centos8")
client.connect(mqttBroker,port=18160)

print('stating the loop and saving to file')



while True:
    client.loop_start()


    client.subscribe("net1")
#print('on message ')
    client.on_message=on_message



    time.sleep(30)
    #print('start 30 sec sleep')
    client.loop_stop()

