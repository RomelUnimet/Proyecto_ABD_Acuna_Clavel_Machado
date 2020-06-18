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


# THIRD QUESTION.
# QUESTION: 
# ¿Qué horas del día son más rentable para cada una de las tiendas? Razone su respuesta.
# ANSWER:
# Para saber qué horas del día son más rentables en cada una de las tiendas basta con correr el método "mostProfitableHours".
# Este método devuelve un mensaje por cada tienda que indica a qué hora tiene el promedio más alto de cantidad facturada ($)
# y la hora donde existen más clientes. Para esto se vale de un query que agrupa por hora las facturas y saca el promedio
# de su total y cuenta la cantidad de facturas (que en este caso representarían la cantidad de clientes comprando).
# Cabe destacar que la hora donde el promedio de facturación es mayor es el dato más relevante, ya que este promedio toma
# en cuenta el total facturado en cada hora y la cantidad de facturas en dicha hora. Así que es una métrica que ofrece
# información más relevante.

def mostProfitableHours():

    # "stores" stores the id values of every store.
    stores=[]
    # "messages" stores the information of every store.
    messages=[]

    c = conn.cursor()
    row = c.execute("select _id from plaza.store")
    rows = c.fetchall()
    for row in rows:
        stores.append(row[0])

    for store in stores:
        query = f"""
            WITH maxClients AS (
                SELECT CAST(EXTRACT(HOUR FROM datetime) AS INT) AS hour
                FROM plaza.bill
                WHERE id_store = {store}
                GROUP BY hour
                ORDER BY COUNT(total) DESC
                LIMIT 1
            ),
            maxProfit AS (
                SELECT CAST(EXTRACT(HOUR FROM datetime) AS INT) AS hour
                FROM plaza.bill
                WHERE id_store = {store}
                GROUP BY hour
                ORDER BY ROUND(AVG(total), 2) DESC
                LIMIT 1
            )
            SELECT maxProfit.hour AS maxProfit, maxClients.hour AS maxClients FROM maxClients, maxProfit
        ;"""
        row = c.execute(query)
        rows = c.fetchall()
        for row in rows:
            messages.append(f'La tienda {store}, en promedio, factura más (Bs) a las {row[0]} y tiene un mayor flujo de clientes a las {row[1]}')

    for message in messages:
        print(message)

# UNCOMMENT THE LINE BELOW TO RUN THE FUNCTION.
# mostProfitableHours()




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

# UNCOMMENT THE LINE BELOW TO RUN THE FUNCTION.
# yesterdayInStock(1,3)