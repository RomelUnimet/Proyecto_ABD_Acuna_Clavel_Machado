# IMPORTANTE.
# Se recomienda correr el archivo de jupyter notebook donde aparecen los stored procedures hechos aquí también, ya que 
# los resultados se muestran de una manera más organizada.
# El primer procedimiento almacenado solo se encuentra en este archivo.


import psycopg2
import datetime
import json

# Connect to de DB
conn = psycopg2.connect(
    host='drona.db.elephantsql.com',
    user ='ftuzkdcj',
    password='7UHxXzyMvKwsIqOa9nnC8frDFsesnn6U',
    database='ftuzkdcj'
)

def select(query):
    cur = conn.cursor()
    try:
        cur.execute(query)
    except Exception as e:
        conn.commit()
        print('Error en el query:', e)
    else:
        records = cur.fetchall()
        cur.close()
        return records

#Procedimiento 1: 

#Para este procedimiento se crea un nuevo type en la base de datos.
def createCart():
    query = """ CREATE TYPE plaza.cart AS (product VARCHAR(30), qty INTEGER); """
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()
#createCart()

#Luego se realiza la función de compra
def buyProcedure(product, bank, ci_client, date_transaction, store):

    query=""" CREATE OR REPLACE FUNCTION plaza.shop (product plaza.cart[], bank varchar(15), ci_client varchar(20), date_transaction timestamp,
    store integer)
    RETURNS void AS $$ 
    DECLARE
        bill integer;
    BEGIN
    
        IF array_length(product, 1) > 0 THEN
            INSERT INTO plaza.bill (client_ci, id_store, account, datetime, total)
            VALUES (ci_client, store, bank, date_transaction, 0) RETURNING _id INTO bill;

            INSERT INTO plaza.bill_product(bill_id,  product_name, id_store, quantity)
            SELECT bill, c.product, store, c.qty FROM UNNEST(product) AS c;
        ELSE
            RAISE EXCEPTION 'The cart is empty or null %', now();
        END IF;
    END;
    $$ 
    LANGUAGE plpgsql  """
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

    query = f""" SELECT plaza.shop(array{product}::plaza.cart[], '{bank}', '{ci_client}', '{date_transaction}', '{store}')"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit() 

#mock client
# client = '{"client": "Victoria", "list":[{"prod": "Pera", "qty":10}, {"prod": "Manzana", "qty":69}]}'

# data = json.loads(client)
# car = []
# for i in data['list']:
#     car.append((i['prod'], i['qty']))

# buyProcedure(car, 'Provincial', 'e27111000', '2020-06-15', 1)


#Procedimiento 2:
def account_state(curr_day):
    query=""" CREATE OR REPLACE FUNCTION plaza.account_state (curr_day timestamp)
    RETURNS TABLE (bank varchar(15), balance numeric(32,2), date_meassured date ) AS $$
    DECLARE
        x RECORD;
        banks varchar[] := '{"Provincial", "Mercantil", "Banesco"}';
        aux numeric(32,2);
        i varchar;
    BEGIN

        FOREACH i IN ARRAY banks LOOP
            bank:= i;
            IF i IN ( SELECT account FROM plaza.bill AS b 
                        WHERE DATE(datetime) <= DATE(curr_day) ) THEN
                            SELECT SUM(total) INTO aux FROM plaza.bill WHERE account=i AND 
                            DATE(datetime) <= DATE(curr_day);
                            balance:=aux;
                            date_meassured := curr_day;
                            aux:=0;
                            RETURN NEXT;
            ELSE 
                balance:=0.00;
                date_meassured := curr_day;
                RETURN NEXT;
            END IF;
        END LOOP;
    END;
    $$ LANGUAGE plpgsql"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

    
    query = f""" SELECT * FROM plaza.account_state('{curr_day}') """
    print(select(query))
# UNCOMMENT THE LINE BELOW TO RUN THE FUNCTION.
#account_state(datetime.datetime.now())


#Procedimiento 3:
def clients_points_state(last_date_month):

    print(type(last_date_month))
    query = """ CREATE OR REPLACE FUNCTION plaza.client_points_state (last_date_month timestamp)
 RETURNS TABLE (loyal_client varchar(50), points INTEGER) AS $$ 
 DECLARE 
     X RECORD;
	 last_d date;
 BEGIN
     
	SELECT (date_trunc('MONTH', DATE(last_date_month)) + INTERVAL '1 month' - INTERVAL '1 day')::date INTO last_d;
	 
	IF EXTRACT(DAY FROM last_date_month) = EXTRACT(DAY FROM last_d) THEN
		 FOR X IN (SELECT DISTINCT CONCAT(c.name, ' ', c.last_name) AS full_name, m.points AS pts FROM plaza.membership AS m 
		 INNER JOIN plaza.bill AS b ON m.ci = b.client_ci 
		 INNER JOIN plaza.client AS c ON c.ci = m.ci
		 WHERE c.ci IN (SELECT client_ci FROM plaza.bill WHERE datetime > last_date_month - INTERVAL '1 month' 
		 AND datetime < last_date_month + INTERVAL '1 day') ORDER BY pts DESC) LOOP 
			 loyal_client:= X.full_name;
			 points:= X.pts;
			 RETURN NEXT;
		END LOOP;   
	ELSE 
		RAISE EXCEPTION 'Function meant to be used in the last day of the month %', now();
	END IF;
 END;
 $$ 
 LANGUAGE plpgsql"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

    query = f"""SELECT * FROM plaza.client_points_state('{last_date_month}');"""
    print(select(query))
# UNCOMMENT THE LINE BELOW TO RUN THE FUNCTION.
#clients_points_state('2020-06-04')


def update_prices(d):

    query = """ CREATE OR REPLACE FUNCTION plaza.update_prices (d timestamp)
    RETURNS void 
    AS $$ 
    DECLARE 
        X RECORD;
    BEGIN
	
		FOR X IN (SELECT date AS fe, id_store, product_name, price, cost FROM plaza.price as pBig WHERE date = 
				 (SELECT MAX(date) FROM plaza.price AS pS WHERE pBig.product_name = pS.product_name AND pBig.id_store = pS.id_store)
				 GROUP BY product_name, id_store, price, date) LOOP
					INSERT INTO plaza.price VALUES (DATE(d), X.id_store, X.product_name, X.price*1.05, X.cost*1.05);
				 END LOOP;
    END;
    $$ 
    LANGUAGE plpgsql; """

    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

    query = f""" SELECT plaza.update_prices('{d}')"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit() 

# UNCOMMENT THE LINE BELOW TO RUN THE FUNCTION.
# update_prices('2020-06-23 08:08:08.08')
