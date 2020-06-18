import psycopg2

# Connect to de DB
conn = psycopg2.connect(
    host='drona.db.elephantsql.com',
    user ='ftuzkdcj',
    password='7UHxXzyMvKwsIqOa9nnC8frDFsesnn6U',
    database='ftuzkdcj'
)

host='drona.db.elephantsql.com'
user ='ftuzkdcj'
password='7UHxXzyMvKwsIqOa9nnC8frDFsesnn6U'
dbname='ftuzkdcj'

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



# FOURTH QUESTION.
# QUESTION: 
# ¿Es posible ver la cantidad de mercancía que había por hora en uno de los estantes de una de las tiendas en el día anterior?
# ANSWER:
# Sí, es posible. De la forma en la que está modelada la Base de Datos, existe una entidad "in_stock" que 
# guarda la disponibilidad de cada estante ("shelf"). Es una entidad transaccional que cada vez que una persona
# toma un producto de un estante, se agrega un registro. Como guarda la fecha y hora cuando se realizó, es posible hacerlo.


def yesterdayInStock(id_store, shelf_id):
    # stored procedure is created...
    query="""

        CREATE OR REPLACE FUNCTION yesterdayInStock(id_store_ integer, shelf_id_ integer) 
        RETURNS TABLE (shelf_id integer, product_name character varying, date date, hour numeric, average integer) 
        AS $$
        BEGIN
            RETURN QUERY 
                SELECT plaza.in_stock.shelf_id AS shelf_id, plaza.shelf.product_name AS product_name, 
                    datetime::date AS date, CAST(EXTRACT(HOUR FROM datetime) AS NUMERIC) AS hour, 
                CAST(AVG(qty_available) AS INT) AS average
                FROM plaza.in_stock
                INNER JOIN plaza.shelf ON plaza.shelf._id = plaza.in_stock.shelf_id 
                WHERE '2020-06-11'::date - datetime::date = 1 AND
                    plaza.in_stock.id_store = id_store_ AND 
                    plaza.in_stock.shelf_id = shelf_id_
                GROUP BY plaza.in_stock.shelf_id, date, hour, plaza.shelf.product_name
                ORDER BY plaza.in_stock.shelf_id, date ASC, hour ASC;
        END;
        $$ LANGUAGE plpgsql;

    ;"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

    query=f"SELECT * FROM yesterdayInStock({id_store}, {shelf_id})"
    print(select(query))


# Con esto se sabrá el promedio por hora de la disponibilidad del producto en el estante ("shelf") que se especifique de la
# tienda ("store") elegida. Las horas que no aparecen es porque se mantuvo igual que el registro anterior.

# En este caso, especificó cuál fue el comportamiento de la disponibilidad por hora en el estante 3 de la tienda 1.
yesterdayInStock(1,3)