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

#CLIENTES DE LOS PUBLICADORES// SON 3  CAMARA SHELF Y FRIO
host_pub = "broker.hivemq.com"
#Client con qos 0
clientmqtt_0 = mqtt.Client("Publisher_QoS_0", False)
clientmqtt_0.qos = 0
clientmqtt_0.connect(host=host_pub)
#Client con qos 1
clientmqtt_2 = mqtt.Client("Publisher_QoS_2", False)
clientmqtt_2.qos = 2
clientmqtt_2.connect(host=host_pub)

#PUBLICAR client.publish("Sambil/Camaras/Salida", json.dumps(payload), qos=0)

def on_connect():
    print("Pub connected!")

def main():

    #poner times en hora de apertura del ultimo dia que se hizo una transaccion
    time1=get_latest_time_1()
    time2=get_latest_time_2()


    #Tiempos con los que comienza la simulacion
    #PONER UN IF PARA QUE COMIENCEN EL MISMO DIA AJURO
    time_s_1=datetime.datetime.now().replace(year=time1["year"],month=time1["month"],day=time1["day"])
    time_s_2=datetime.datetime.now().replace(year=time2["year"],month=time2["month"],day=time2["day"])

    #HAY QUE HACER LA MIERDA DE QUE AGARRE SOLAMENTE AL MAYOR PORQUE SINO NO CUADRA

    #definir los productos de cada tienda
    #nombre y stock
    store_prod_1=[]
    store_prod_2=[]

    #funciones que traigan los tiempos
    hours1=get_open_close_1()
    hours2=get_open_close_2()
    
    time_open_1=hours1["opening"]
    time_open_2=hours2["opening"]
    time_close_1=hours1["closing"]
    time_close_2=hours2["closing"]

    #listas que actuan como colas 
    #Siempre inician vacias
    cola_busq_1=[]
    cola_compra_1=[]
    cola_espera_1=[]
    cola_busq_2=[]
    cola_compra_2=[]
    cola_espera_2=[]

    #funciona que me traiga la maxima capacidad
    max_cap_1=get_max_in_1()
    max_cap_2=get_max_in_2()

    #var de la gente adentro // siempre empieza en 0
    people_in_1=0
    people_in_2=0

    while(True):

        print("INICIO DEL DIA")

        #reiniciar array clientes
        clients=get_clients()

        #CAMBIAMOS LA HORA DE LA FECHA A LA HORA DE APERTURA
        time_s_1=time_s_1.replace(hour=time_open_1)
        time_s_2=time_s_2.replace(hour=time_open_2)


        #loop con la duracion del dia
        while(time_s_1.hour>time_close_1 and time_s_2.hour>time_close_2 and len(clients)!=0): #si no hay clientes disponibles se acaba el dia sabrosamente 
            
            #if de la tienda 1
            if(time_s_1.hour<time_close_1): 
                #simulacion entrada random de clientes

                #generamos la cantidad
                cant=random.randint(1,3) #np.random.normal(media,dist) no puede ser neg

                client_select=[]
                #sacamos de manera random del array de clientes
                for x in range(cant):
                    random.shuffle(clients)
                    cl=clients.pop()
                    client_select.append(cl)
                    
                #loop en donde se mete en cada cola
                for x in client_select:
                    if(people_in_1<max_cap_1): 

                        cola_busq_1.append(x)
                        print(x+" entro a la tienda")


                        #PUBLICADOR HACE DE LAS SUYAS
                        #ENVIA MENSAJE CON LA PERSONA QUE ENTRO A LA TIENDA
                        payload={
                            "ci":x,
                            "store":1,
                            "datetime":time_s_1
                        }

                        clientmqtt_0.publish('Plaza/camera/tienda_1',json.dumps(payload),qos=0)


                        people_in_1=people_in_1+1
                        print("people in store 1" + str(people_in_1) )
                    else:
                        cola_espera_1.append(x)
                        print(x+" entro a la cola de espera")
                



                #CHEQUEO DE LA TEMP
                #TRAE TODO LOS SHELF QUE TENGAN TEMP CON SU TEMP ACTUAL Y LA SUPUESTA
                shelf_temp=get_shelf_temp_1()
                for x in shelf_temp:

                    up=random.randint(0,9)
                    if up==0:
                        x["temp_actual"]=x["temp_actual"]+1
                    
                    #PUBLICADOR HACE DE LAS SUYAS
                    payload={
                        "shelf_id":x["shelf_id"],
                        "id_store":1,
                        "datetime":time_s_1,
                        "temp_actual":x["temp_actual"],
                        "min_temp":x["min_temp"]
                    }

                    clientmqtt_0.publish('Plaza/shelf_temperature/tienda_1',json.dumps(payload),qos=0)
                    



                #CICLO DE BUSQUEDA Y COMPRA
                limite_tiempo=15 #sacar despues cual seria buena en MINUTOS FIJO
                tiempo_trans=0 #tiempo que transcurre en MINUTOS/ EMPIEZA EN 0 #tiene que trancurrir el tiempo


                while((len(cola_busq_1)!=0 or len(cola_compra_1)!=0) and tiempo_trans<limite_tiempo):
                    #if cola busqueda
                    if(len(cola_busq_1)!=0):

                        #Todavia falta
                        store_prod_1=get_prod_1() #se saca el array con la actializacion de los stocks en la base de datos

                        cl_b=cola_busq_1.pop()
                        prod_list=[] #productos del cliente

                        cant_prod=random.randint(1,5) #np.random.normal(media,dist) no puede ser neg

                        #generamos random 
                        for x in range(cant_prod):
                            random.shuffle(store_prod_1)
                            prod=store_prod_1.pop()

                            max_cant_prod=prod["stock"]
                            quantity=random.randint(1,max_cant_prod) #np.random.normal(media,dist) no puede ser 0 y tiene que ser menos que el stock maximo que se tiene



                            n={
                                "prod":prod["name"], #nombre del prod
                                "quantity":quantity #la cantidad del producto que se toma
                            }


                            prod_list.append(n)
                            tiempo_trans=tiempo_trans + 1 #"1 minuto por cada producto o algo asi" 
                            time_s_1=time_s_1+datetime.timedelta(minutes=1)
                            

                            new_stock_prod=max_cant_prod-quantity


                            #Hacer metodo de update al stock y le pasamos (new stock prod)

                            payload={
                                "shelf_id":prod["shelf_id"],
                                "id_store":1,
                                "datetime":time_s_1,
                                "qty_available":new_stock_prod,
                                "max":prod["max"]
                            }

                            clientmqtt_2.publish('Plazas/stock/tienda_1',json.dumps(payload),qos=2)
                            #PUBLICADOR MANDA LA SEÑAL DEL STOCK DEL SHELF
                            #SEÑAL SERA CON EL PROD Y EL NUEVO STOCK Y COMPARA LO DEL 20%
                        

                        #ver si uso dict
                        client_comp={
                            "client":cl_b,#cedula
                            "list":prod_list
                        }

                        print(str(client_comp["client"])+" obtuvo los productos "+ str(client_comp["list"]))
                        cola_compra_1.append(client_comp) #seria el cliente con su array de productos


                    #if cola compra
                    if(len(cola_compra_1)!=0):
                        cl_comp=cola_busq_1.pop()

                        #PA CON EL CLIENTE DE CL_COMP


                        people_in_1=people_in_1-1
                        tiempo_trans=tiempo_trans + len(cl_comp["list"]) #"1 minuto por cada producto o algo asi" 
                        time_s_1=time_s_1+datetime.timedelta(minutes=len(cl_comp["list"]))

                        if (len(cola_espera_1)!=0):
                            cola_busq_1.append(cola_espera_1.pop())
                        print(cl_comp["client"] + "realizo su compra")
                    

                    #PONER MANERA DE AVANZAR TIEMPO AUNQUE NO HAYAN CLIENTES EN LAS COLAS      
                print("Pasaron: "+ str(tiempo_trans)+" minutos en la tienda 1" )
            


            #if de la tienda 2







        #CREO QUE ES AQUI  NO ESOTY SEGURO
        #PA FIN DEL DIA
        if(False): #es un mes se hace el PA del mes
            pa_del_mes=0

        #FALTA MANERA DE HACER QUE NO PASE UN DIA
        time_s_1=time_s_1+"un dia"

#FALA HACER LO QUE APSA CUANDO SE ACERCA LA HORA DE CERRAR Y EL CICLO CON T2 EN LA SIMULACION





       

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

def  get_latest_time_2():
    
    sql='''
            select  EXTRACT(DAY from b."datetime") as day, 
                    EXTRACT(MONTH from b."datetime") as month, 
                    EXTRACT(YEAR from b."datetime") as year
            from plaza.bill as b 
            where b."id_store"=2 
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

def get_clients():
    sql=''' 
            select c."ci" from plaza.client as c

    '''    
    df = pd.read_sql_query(sql, conn)
    lista = []
    for index, row in df.iterrows():
        lista.append(row["ci"])
    print(lista)
    return lista

#FALTA TERMINARLO CUANDO YA TENGA EL NOMBRE
def get_open_close_1():
    
    sql='''
            select s."opening", s."closing" from plaza.store as s
            where s."_id"=1
    
    '''
    
    df = pd.read_sql_query(sql, conn)

    for index, row in df.iterrows():

        x={
            "opening":row["opening"].hour,
            "closing":row["closing"].hour
        }
        

    print(x)  

    return x

def get_open_close_2():

    sql='''
            select s."opening", s."closing" from plaza.store as s
            where s."_id"=2
    
    '''
    
    df = pd.read_sql_query(sql, conn)

    for index,row in df.iterrows():

        x={
            "opening":row["opening"].hour,
            "closing":row["closing"].hour
        }
        

    print(x)

    return x  #x     

#PUEDE SER SOLO UNA
def get_max_in_1():
    
    sql='''
            select s."max_people" from plaza.store as s 
            where s."_id"=1
        '''
    
    df = pd.read_sql_query(sql, conn)


    for index, row in df.iterrows():
        cant_max=(row["max_people"])
    print(cant_max)

    return cant_max #cant_max

def get_max_in_2():

    sql='''
            select s."max_people" from plaza.store as s 
            where s."_id"=2
        '''
    
    df = pd.read_sql_query(sql, conn)


    for index, row in df.iterrows():
        cant_max=(row["max_people"])
    print(cant_max)

    return cant_max #cant_max

def get_prod_1():
    
    sql='''
            select p."name", i."qty_available",s."_id",s."capacity"  from plaza."product" as p
            inner join plaza.shelf as s on s."product_name"=p."name"
            inner join plaza.in_stock as i on s."_id"=i."shelf_id"
            where p."id_store"=1 and
                  s."id_store"=1 and
                  i."id_store"=1 and
                  i."datetime"= (

                      select max(plaza.in_stock."datetime") from plaza.in_stock
                      inner join  plaza.shelf as sh on plaza.in_stock."shelf_id"= sh."_id"
                      where sh."product_name"=p."name" and
                            sh."id_store"=1 and
                            p."id_store"=1
                  )
    '''
    df = pd.read_sql_query(sql, conn)

    lista=[]

    for index, row in df.iterrows():
        p={
            "prod":row["name"],
            "stock":row["qty_available"],
            "shelf_id":row["_id"],
            "max":row["capacity"]
        }
        lista.append(p)
        
    print(lista)

    return lista 

def get_prod_2():
    
    sql='''
            select p."name", i."qty_available",s."_id",s."capacity"  from plaza."product" as p
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
            "max":row["capacity"]
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

def get_shelf_temp_2():
    sql='''
            select t."shelf_id", t."id_store",t."datetime",t."temperature",s."min_temperature"
            from plaza.temperature as t
            inner join plaza.shelf as s on s."_id"=t."shelf_id"
            where t."id_store"=2 and 
                  s."id_store"=2 and 
                  t.datetime =(
                                    select max(temp."datetime") from plaza.temperature as temp
                                    where  temp."shelf_id"=t."shelf_id" and
                                           temp.id_store=2

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
           

