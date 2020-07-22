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


host='ruby.db.elephantsql.com'
user ='fvhavaif'
password='THCA_nW8eWwmkuQ4mkobpS0qvZNLEYzE'
dbname='fvhavaif'

conn=psy.connect(host=host, user=user, password=password, dbname=dbname)

host_pub = "broker.hivemq.com"
#Client con qos 0




#CONNECT DE LAS CAMARAS DE ENTRADA
def on_connect(client, userdata, flags, rc):    
    print('Conectado (%s)' % client._client_id)
    client.subscribe(topic='Plazas/#', qos = 0) 



#falta probarlo
def on_message_camera(client, userdata, message):   

    #payload tiene ci,store,datetime
    a = json.loads(message.payload)

    print("Entro un cliente") 
    print(a) 

    cur = conn.cursor()                             
    cur.execute("INSERT INTO ventas.visit (client_ci, id_store, datetime) VALUES (%s, %s, %s);",
                (a["ci"],a["store"],a["datetime"]))
    conn.commit()

    #INSERTAMOS LA VISITA DEL CLIENTE

def on_message_stock(client, userdata, message):  

    #payload shelf_id, id_store, date, qty_available,max
    a = json.loads(message.payload)

    clientmqtt = mqtt.Client("Publisher_QoS", False)
    clientmqtt.qos = 0
    clientmqtt.connect(host=host_pub)

    print("Stock de Shelf") 
    print(a) 

    cur = conn.cursor()                             
    cur.execute("INSERT INTO inventario.in_stock (shelf_id, id_store, datetime,qty_available) VALUES (%s, %s, %s, %s);",
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
        print('is 20%')
        clientmqtt.publish('Plazas/restock/'+str(1) ,json.dumps(payload),qos=0)     


    #payload={
    #    "funcionno":"funciono"
    #}

   #PEUBA DE SI SE MANDAN DOS
    #time.sleep(0.5)
    #print('antes')
    
    #print('despues')


    #I

def on_message_restock(client, userdata, message):   
    a = json.loads(message.payload)
    print("Restock de shelf") 
    print(a) 

    cur = conn.cursor()                             
    cur.execute("INSERT INTO inventario.restock (shelf_id, id_store, datetime) VALUES (%s, %s, %s);",
                (a["shelf_id"],a["id_store"],a["datetime"]))
    conn.commit()  

    aux=datetime.datetime.strptime(a["datetime"], '%Y-%m-%d %H:%M:%S.%f')
    var=datetime.datetime.now().replace(year=aux.year,month=aux.month,day=aux.day,hour=aux.hour,minute=aux.minute,second=aux.second)
    var=var+datetime.timedelta(seconds=1)


    cur = conn.cursor()                             
    cur.execute("INSERT INTO inventario.in_stock (shelf_id, id_store, datetime,qty_available) VALUES (%s, %s, %s, %s);",
               (a["shelf_id"],a["id_store"],var,a["max"]))
    conn.commit()   

def on_message_shelf_temp(client, userdata, message):
    
    #payload shelf_id, id_store, datetime, temp_actual, min_temp   
    a = json.loads(message.payload)
    print("Shelf temperature") 
    print(a) 

    clientmqtt = mqtt.Client("Publisher_QoS", False)
    clientmqtt.qos = 0
    clientmqtt.connect(host=host_pub)

    cur = conn.cursor()                             
    cur.execute("INSERT INTO inventario.temperature (shelf_id, id_store, datetime,temperature) VALUES (%s, %s, %s, %s);",
                (a["shelf_id"],a["id_store"],a["datetime"],a["temp_actual"]))
    conn.commit()   

    var=abs(a["min_temp"]-a["temp_actual"])
    if var>=3:
        print('temp cambio mas de 3 grados')
        payload={
            "shelf_id":a["shelf_id"],
            "id_store":a["id_store"],
            "datetime":a["datetime"],
            "temp_actual":a["temp_actual"],
            "min_temp":a["min_temp"]
        }
        time.sleep(0.5)
        clientmqtt.publish('Plazas/fixtemp/'+str(1) ,json.dumps(payload),qos=0)     

def on_message_fix_temp(client, userdata, message):

    #payload shelf_id, id_store, datetime, temp_actual, min_temp   
    a = json.loads(message.payload)
    print("Fix temperature") 
    print(a) 

    aux=datetime.datetime.strptime(a["datetime"], '%Y-%m-%d %H:%M:%S.%f')
    var=datetime.datetime.now().replace(year=aux.year,month=aux.month,day=aux.day,hour=aux.hour,minute=aux.minute,second=aux.second)
    var=var+datetime.timedelta(seconds=1)

    cur = conn.cursor()                             
    cur.execute("INSERT INTO inventario.temperature (shelf_id, id_store, datetime,temperature) VALUES (%s, %s, %s, %s);",
                (a["shelf_id"],a["id_store"],var,a["min_temp"]))
    conn.commit()   

        
    

def main():	

    host = "broker.hivemq.com"

    client_1 = paho.mqtt.client.Client(client_id='Subscriptor Plazas',clean_session=False)
    #client_stock = paho.mqtt.client.Client(client_id='Shelf inteligente',clean_session=False)
    #client_restock = paho.mqtt.client.Client(client_id='Shelf inteligente',clean_session=False)
    #client_shelf_temp = paho.mqtt.client.Client(client_id='Temperatura de los Shelf',clean_session=False)
    #client_fix_temp = paho.mqtt.client.Client(client_id='Temperatura de los Shelf',clean_session=False)

    client_1.on_connect = on_connect
    #client_stock.on_connect = on_connect_stock
    #client_restock.on_connect = on_connect_restock
    #client_shelf_temp.on_connect = on_connect_temp
    #client_fix_temp.on_connect = on_connect_fix_temp

    client_1.message_callback_add('Plazas/camera/#', on_message_camera)
    client_1.message_callback_add('Plazas/stock/#', on_message_stock)
    client_1.message_callback_add('Plazas/restock/#', on_message_restock)
    client_1.message_callback_add('Plazas/shelf_temperature/#', on_message_shelf_temp)
    client_1.message_callback_add('Plazas/fixtemp/#', on_message_fix_temp)

    client_1.connect(host=host) 
    #client_stock.connect(host=host)  
    #client_restock.connect(host=host)  
    #client_shelf_temp.connect(host=host) 
    #client_fix_temp.connect(host=host) 

    client_1.loop_forever()
    #client_stock.loop_forever()
    #client_restock.loop_forever()
    #client_shelf_temp.loop_forever()
    #client_fix_temp.loop_forever()


if __name__ == '__main__':
   main()