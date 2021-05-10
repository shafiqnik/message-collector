'''
Supporting libraries
dnspython 2.0.0
paho-mqtt 1.5.1
pip 20.2.3
setuptools 49.2.1
tinydb 4.4.0
'''


#!/usr/bin/python3

from datetime import date

import datetime
import time
import sys
import os
import sqlite3


from time import gmtime, strftime

sys.path.insert(0, "c:/python39/lib/site-packages")  #include the library path to system path
import paho.mqtt.client as mqtt  #import paho mqtt
from tinydb import TinyDB, Query


mytime = str(strftime("%Y-%m-%d %H:%M:%S", gmtime()))


buffer =[]
timestamp = str(datetime.datetime.now())
dbtable_id = 1

print('starting the script....'+timestamp)

'''
STORING THE MESSAGES IN A LOCAL DB
START OF DB CODE
'''

db = TinyDB('c:/Users/shafiqah/PycharmProjects/message-collector/jc3.json')
User = Query()

#2021-05-07 06:55:08.130419{"data":["$GPSR,F008D1798D5C,F008D1798D5C,-127,210507,075116.00,49.259453,-122.915359,0.0,1.4,1620373875"]}

def dbinsert(tableName,jcid,bleMessage,rtc):
    db.insert({'name':tableName,'id':jcid,'bleMessage':bleMessage,'rtc':rtc})
    print('wrote to db')

def dbsearch(tableName):
    print(db.search(User.name == tableName))


#dbsearch('98F4AB891894')
'''
END OF DB CODE
'''

def on_message(client, userdata, message):
    ble_payload = str(message.payload.decode("utf-8"))
    print("received message: " ,ble_payload)
    #f = open(timestamp+'.csv', 'a')
    f = open('c:/Temp0/something.csv', 'a')
    f.write(str(datetime.datetime.now()))
    f.write(ble_payload)
    f.write('\n')

    mymessage = ble_payload.split(',')
    if len(mymessage) > 7:
        dbinsert(mymessage[2],mymessage[1],mymessage[3:10], mymessage[-1])
    else:
        dbinsert(mymessage[2],mymessage[1],mymessage[4],mymessage[-1])


    buffer.append(str(message.payload.decode("utf-8")))

    f.close()

mqttBroker ="104.36.12.156"
#mqttBroker="10.220.252.10"


client = mqtt.Client("laptop")
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

