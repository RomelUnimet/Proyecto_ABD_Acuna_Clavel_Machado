import ssl
import sys
import json
import random
import time
import paho.mqtt.client as mqtt
import paho.mqtt.publish
import numpy as np
import datetime
import psycopg2 as psy
import pandas as pd


host='drona.db.elephantsql.com'
user ='ftuzkdcj'
password='7UHxXzyMvKwsIqOa9nnC8frDFsesnn6U'
dbname='ftuzkdcj'

conn=psy.connect(host=host, user=user, password=password, dbname=dbname)

host_pub = "broker.hivemq.com"
#Client con qos 0
clientmqtt_0 = mqtt.Client("Publisher_QoS_0", False)
clientmqtt_0.qos = 0
clientmqtt_0.connect(host=host_pub)
#Client con qos 1
clientmqtt_2 = mqtt.Client("Publisher_QoS_2", False)
clientmqtt_2.qos = 2
clientmqtt_2.connect(host=host_pub)


#CONNECT DE LAS CAMARAS DE ENTRADA
def on_connect_camera(client, userdata, flags, rc):    
    print('Conectado (%s)' % client._client_id)
    client.subscribe(topic='Plazas/camera/#', qos = 0) 

#CONNECT DE EL STOCK DEL PRODUCTO
def on_connect_stock(client, userdata, flags, rc):    
    print('Conectado (%s)' % client._client_id)
    client.subscribe(topic='Plazas/stock/#', qos = 2) 

#CONNECT DE EL RESTOCK DEL PRODUCTO
def on_connect_restock(client, userdata, flags, rc):    
    print('Conectado (%s)' % client._client_id)
    client.subscribe(topic='Plazas/restock/#', qos = 2) 

#CONNECT DE LA TEMPERATURA DE LOS ESTANTES FRIOS
def on_connect_temp(client, userdata, flags, rc):    
    print('Conectado (%s)' % client._client_id)
    client.subscribe(topic='Plazas/shelf_temperature/#', qos = 0) 
#CONNECT DE LA TEMPERATURA DE LOS ESTANTES CUANDO SE TIENE QUE PONER OTRA VEZ   
def on_connect_fix_temp(client, userdata, flags, rc):    
    print('Conectado (%s)' % client._client_id)
    client.subscribe(topic='Plazas/fix_temp/#', qos = 0) 


#falta probarlo
def on_message_camera(client, userdata, message):   

    #payload tiene ci,store,datetime
    a = json.loads(message.payload)

    print(a) 

    cur = conn.cursor()                             
    cur.execute("INSERT INTO plaza.visit ('client_ci', 'id_store', 'datetime') VALUES (%s, %s, %s);",
                (a["ci"],a["id_store"],a["datetime"]))
    conn.commit()

    #INSERTAMOS LA VISITA DEL CLIENTE

def on_message_stock(client, userdata, message):  

    #payload shelf_id, id_store, date, qty_available(lo que sacamos que ser a lo que tiene ahora),max
    a = json.loads(message.payload)
    print(a) 

    cur = conn.cursor()                             
    cur.execute("INSERT INTO plaza.in_stock ('shelf_id', 'id_store', 'datetime','qty_available') VALUES (%s, %s, %s, %s);",
                (a["shelf_id"],a["id_store"],a["datetime"],a["qty_available"]))
    conn.commit()    

    percent=(a["qty_available"]/a["max"])*100

    payload={
        "shelf_id":a["shelf_id"],
        "id_store":a["id_store"],
        "datetime":a["datetime"],
        "max":a["max"]
    }

    if percent<20:
        clientmqtt_2.publish('Plazas/restock/tienda'+ str(a["id_store"]) ,json.dumps(payload),qos=2)

    #I

def on_message_restock(client, userdata, message):   
    a = json.loads(message.payload)
    print(a) 

    cur = conn.cursor()                             
    cur.execute("INSERT INTO plaza.restock ('shelf_id', 'id_store', 'datetime') VALUES (%s, %s, %s);",
                (a["shelf_id"],a["id_store"],a["datetime"]))
    conn.commit()  

    var=datetime.datetime.now().replace(year=a["datetime"].year,month=a["datetime"].month,day=a["datetime"].day,hour=a["datetime"].hour,minute=a["datetime"].minute,second=a["datetime"].second)
    var=var+datetime.timedelta(seconds=1)


    cur = conn.cursor()                             
    cur.execute("INSERT INTO plaza.in_stock ('shelf_id', 'id_store', 'datetime','qty_available') VALUES (%s, %s, %s, %s);",
                (a["shelf_id"],a["id_store"],var,a["max"]))
    conn.commit()   

def on_message_shelf_temp(client, userdata, message):
    
    #payload shelf_id, id_store, datetime, temp_actual, min_temp   
    a = json.loads(message.payload)
    print(a) 

    cur = conn.cursor()                             
    cur.execute("INSERT INTO plaza.temperature ('shelf_id', 'id_store', 'datetime','temperature') VALUES (%s, %s, %s, %s);",
                (a["shelf_id"],a["id_store"],a["datetime"],a["temp_actual"]))
    conn.commit()   

    var=a["min_temp"]-a["temp_actual"]
    if var>=3:
        payload={
            "shelf_id":a["shelf_id"],
            "id_store":a["id_store"],
            "datetime":a["datetime"],
            "temp_actual":a["temp_actual"],
            "min_temp":a["min_temp"]
        }

        clientmqtt_0.publish('Plazas/fix_temp/tienda'+ str(a["id_store"]) ,json.dumps(payload),qos=0)

def on_message_fix_temp(client, userdata, message):

    #payload shelf_id, id_store, datetime, temp_actual, min_temp   
    a = json.loads(message.payload)
    print(a) 

    var=datetime.datetime.now().replace(year=a["datetime"].year,month=a["datetime"].month,day=a["datetime"].day,hour=a["datetime"].hour,minute=a["datetime"].minute,second=a["datetime"].second)
    var=var+datetime.timedelta(seconds=1)

    cur = conn.cursor()                             
    cur.execute("INSERT INTO plaza.temperature ('shelf_id', 'id_store', 'datetime','temperature') VALUES (%s, %s, %s, %s);",
                (a["shelf_id"],a["id_store"],var,a["min_temp"]))
    conn.commit()   

        
    

def main():	

    host = "broker.hivemq.com"

    client_camera = paho.mqtt.client.Client(client_id='Camara entrada supermercado',clean_session=False)
    client_stock = paho.mqtt.client.Client(client_id='Shelf inteligente',clean_session=False)
    client_restock = paho.mqtt.client.Client(client_id='Shelf inteligente',clean_session=False)
    client_shelf_temp = paho.mqtt.client.Client(client_id='Temperatura de los Shelf',clean_session=False)
    client_fix_temp = paho.mqtt.client.Client(client_id='Temperatura de los Shelf',clean_session=False)

    client_camera.on_connect = on_connect_camera
    client_stock.on_connect = on_connect_stock
    client_restock.on_connect = on_connect_restock
    client_shelf_temp.on_connect = on_connect_temp
    client_fix_temp.on_connect = on_connect_fix_temp

    client_camera.message_callback_add('Plazas/camera/#', on_message_camera)
    client_stock.message_callback_add('Plazas/stock/#', on_message_stock)
    client_restock.message_callback_add('Plazas/restock/#', on_message_restock)
    client_shelf_temp.message_callback_add('Plazas/shelf_temperature/#', on_message_shelf_temp)
    client_fix_temp.message_callback_add('Plazas/fix_temp//#', on_message_fix_temp)

    client_camera.connect(host=host) 
    client_stock.connect(host=host)  
    client_restock.connect(host=host)  
    client_shelf_temp.connect(host=host) 
    client_fix_temp.connect(host=host) 

    client_camera.loop_start()
    client_stock.loop_start()
    client_restock.loop_start()
    client_shelf_temp.loop_start()
    client_fix_temp.loop_start()


if __name__ == '__main__':
   main()