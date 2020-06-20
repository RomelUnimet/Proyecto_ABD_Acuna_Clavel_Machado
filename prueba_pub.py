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


def get_prod_1():
    
    sql='''
            select p."name", i."qty_available",s."_id",s."capacity",i."datetime"  from plaza."product" as p
            inner join plaza.shelf as s on s."product_name"=p."name"
            inner join plaza.in_stock as i on s."_id"=i."shelf_id"
            where p."id_store"=2 and
                  s."id_store"=2 and
                  i."id_store"=2 and
                  i."datetime"= (

                      select max(plaza.in_stock."datetime") from plaza.in_stock
                      inner join  plaza.shelf as sh on plaza.in_stock."shelf_id"= sh."_id"
                      where sh."product_name"=p."name" and
                            sh."id_store"=2 and
                            p."id_store"=2
                  )
    '''
    df = pd.read_sql_query(sql, conn)

    lista=[]

    for index, row in df.iterrows():
        p={
            "prod":row["name"],
            "stock":row["qty_available"],
            "shelf_id":row["_id"],
            "max":row["capacity"],
            "date":row["datetime"].second
        }
        lista.append(p)
        
    
    print(lista)


    return lista



def get_shelf_temp_1():
    sql='''
            select t."shelf_id", t."id_store",t."datetime",t."temperature",s."min_temperature"
            from plaza.temperature as t
            inner join plaza.shelf as s on s."_id"=t."shelf_id"
            where t."id_store"=1 and 
                  s."id_store"=1 and 
                  t.datetime =(
                                    select max(temp."datetime") from plaza.temperature as temp
                                    where  temp."shelf_id"=t."shelf_id" and
                                           temp.id_store=1 

                                )
    
    
    '''
    df = pd.read_sql_query(sql, conn)

    lista=[]

    for index, row in df.iterrows():
        p={
            "shelf_id":row["shelf_id"],
            "id_store":row["id_store"],
            "datetime":row["datetime"],
            "temperature":row["temperature"],
            "min_temperature":row["min_temperature"]
        }
        lista.append(p)
        
    print(lista)

    

    return lista

def  get_latest_time_1():
    
    sql='''
            select  EXTRACT(DAY from b."datetime") as day, 
                    EXTRACT(MONTH from b."datetime") as month, 
                    EXTRACT(YEAR from b."datetime") as year
            from plaza.bill as b 
            where b."id_store"=1 
            order by b."datetime" desc 
            limit 1;
    '''

    df = pd.read_sql_query(sql, conn)

    for index ,row in df.iterrows():

        x={
            "day":row["day"],
            "month":row["month"],
            "year":row["year"]
        }
        

    print(x)

    return x

def cuenta():

    a=random.randint(0,2)
    cuenta=""
    if a==0:
        cuenta="Mercantil"
    elif a==1:
        cuenta="Banesco"
    else:
        cuenta="Provincial"
    
    print(cuenta)

    return cuenta

host_pub = "broker.hivemq.com"

clientmqtt = mqtt.Client("Publisher_QoS", False)
clientmqtt.qos = 0
clientmqtt.connect(host=host_pub)


def main():

    payload={
        "shelf_id":7,
        "id_store":1,
        "datetime":str(datetime.datetime.now()),
        "temp_actual":-14,
        "min_temp":-19
    }
    #for x in range(2):
        #print('-------------------------------------------------------------------------')
    time.sleep(0.5)

    clientmqtt.publish('Plazas/shelf_temperature/'+str(1) ,json.dumps(payload),qos=0)     
    #x=abs(payload["min_temp"]-payload["temp_actual"])
    #print(x)


if __name__ == '__main__':
       main()
