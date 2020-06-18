import psycopg2

# Connect to de DB
conn = psycopg2.connect(
    host='drona.db.elephantsql.com',
    user ='ftuzkdcj',
    password='7UHxXzyMvKwsIqOa9nnC8frDFsesnn6U',
    database='ftuzkdcj'
)

#Procedimiento 1: 

#Para este procedimiento se crea un nuevo type en la base de datos.
def createCart():
    query = """ CREATE TYPE cart AS (product VARCHAR(30), qty INTEGER); """
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()


#Luego se realiza la función de compra
def buyProcedure(product, bank, ci_client, date_transaction, store):

    query=""" CREATE OR REPLACE FUNCTION shop (product cart[], bank varchar(15), ci_client varchar(20), date_transaction timestamp,
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

    query = f""" SELECT shop({product}, {bank}, {ci_client}, {date_transaction}, {store})"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()
    



#Procedimiento 2:

def account_state(curr_day):
    query=""" CREATE OR REPLACE FUNCTION account_state (curr_day TIMESTAMP)
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

    
    query = f""" SELECT * FROM account_state({curr_day}) """
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()


#Procedimiento 3:

def clients_points_state(last_date_month):
    query = """ CREATE OR REPLACE FUNCTION client_points_state (last_date_month TIMESTAMP)
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

    query = f"""SELECT * FROM clients_points_state({last_date_month})"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

