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

# FIRST QUESTION.
# QUESTION: 
# ¿Cuáles son las categorías de producto que se venden mejor en cada una de las tiendas? Use un pivot table para razonar su respuesta.
# ANSWER:
# Para obtener la respuesta a esa pregunta, se realiza lo siguiente. Se crea una view que contendrá tres columnas: 
# categoría, tienda y cantidad vendida. Cada tupla representa la cantidad total vendida de un producto (Manzana) en la tienda 
# correspondiente, pero muestra la categoría del producto (Frutas) en vez de su nombre. Las categorías y 
# las tiendas se repiten en varias tuplas. Se puede ver entonces, que tenemos relacionado todo lo que queremos para realizar 
# nuestro análisis, sin embargo, no está dispuesto de la forma más entendible y óptima para análisis. 
# Es por esto que nos valemos de una pivot table, cuya primera columna tiene las dos tiendas; y las categorías ahora son 
# columnas que se expanden horizontalmente y no se repiten, cuyos valores corresponderán al total comprado de todos los 
# productos de esa categoría en la tienda correspondiente. 
# De ese modo, observar cuál categoría se vende mejor en la tienda x, es tan fácil como buscar el mayor valor en la fila 
# correspondiente de esa tienda y observar el nombre de la columna.

def analyzeCategories():

    query = """ CREATE OR REPLACE VIEW plaza.products_cat_selled AS
    SELECT DISTINCT p.category, x.store, x.sold FROM 
    (SELECT SUM(bp.quantity) AS sold, bp.id_store AS store, bp.product_name AS product FROM plaza.bill_product AS bp 
    GROUP BY product,store) AS x INNER JOIN plaza.product AS p ON p.name = x.product ORDER BY store;"""

    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

    query = """SELECT store, COALESCE(Cereales, 0) AS Cereales, COALESCE(Frutas, 0) AS Frutas, COALESCE(Higiene, 0) AS Higiene
    , COALESCE(Congelados, 0) AS Congelados, COALESCE(Snacks, 0) AS Snacks, COALESCE(Panes, 0) AS Panes
    , COALESCE(Granos, 0) AS Granos, COALESCE(Salsas, 0) AS Salsas, COALESCE(Proteinas, 0) AS Proteinas 
    , COALESCE(Vegetales,0) AS Vegetales, COALESCE(Hortalizas,0) AS Hortalizas
    FROM   crosstab(
    'SELECT store, category, SUM(sold)
        FROM plaza.products_cat_selled 
        GROUP BY 1,2 ORDER BY 1,2;' 

    , $$VALUES ('Cereales'::text), ('Frutas'), ('Higiene'), ('Congelados'),
     ('Snacks'), ('Panes'), ('Granos'), ('Salsas'), ('Proteinas'), ('Vegetales'), ('Hortalizas')$$
    ) AS ct (store text, Cereales int, Frutas int, Higiene int, Congelados int, Snacks int, Panes int, Granos int, 
    Salsas int, Proteinas int, Vegetales int, Hortalizas int);"""

    print(select(query))

# UNCOMMENT THE LINE BELOW TO RUN THE FUNCTION.
analyzeCategories()


# SECOND QUESTION.
# QUESTION: 
# ¿Qué banco, categoría de producto y tienda prefieren los clientes de nuestro programa de afiliados? Razone su respuesta.
# ANSWER:
# Para responder esta pregunta creamos un nuevo procedimiento almacenado llamado client_preferences. Este procedimiento retorna
# 4 columnas: los clientes de nuestro programa de afiliados, y, para cada uno: el banco con el que más ha realizado 
# transacciones en cualquiera de las tiendas, cuál es la categoría que corresponde a los productos que más ha comprado, 
# y finalmente la tienda en la que más ha comprado. Tener esta tabla nos da mucha versatilidad para responder la pregunta 
# de varias formas. Podemos ver las preferencias individuales de cada uno de nuestros clientes, y también podemos:
# 1.- Obtener qué banco, categoría de producto, y tienda prefieren nuestros clientes afiliados.
# 2.- Obtener el banco que prefieren nuestros afiliados junto con la cantidad de clientes cuya preferencia es ese banco.
# 3.- Obtener la categoría que prefieren nuestros afiliados junto con la cantidad de clientes cuya preferencia es esa categoría.
# 4.- Obtener la tienda que prefieren nuestros afiliados junto con la cantidad de clientes que prefieren es esa tienda.
# Cada uno de estos querys se presentan debajo de la creación del stored procedure identificados.

# METODO PARA CREAR EL PA
def client_preferences(argument):
    query=""" CREATE OR REPLACE FUNCTION plaza.client_preferences ()
    RETURNS TABLE (client varchar(50), bank_of_preference varchar(15), category_of_preference varchar(20), store_of_preference integer) 
    AS $$ 
    DECLARE 
        X RECORD;
        Y RECORD;
        Z RECORD;
        W RECORD;
    BEGIN
        
        FOR X IN (SELECT CONCAT(c.name, ' ', c.last_name) AS full_name, m.ci AS ci FROM plaza.membership AS m 
        INNER JOIN plaza.client AS c ON c.ci = m.ci) LOOP
        
            FOR Y IN (SELECT account AS name_bank, COUNT(account) AS bank FROM plaza.bill WHERE client_ci = X.ci GROUP BY account
            ORDER BY bank DESC LIMIT 1) LOOP 
            
                FOR Z IN (SELECT DISTINCT p.category AS name_cat, aux.sold FROM 
                (SELECT SUM(bp.quantity) AS sold, bp.product_name AS product FROM plaza.bill_product AS bp 
                WHERE bp.bill_id IN (SELECT _id FROM plaza.bill WHERE client_ci = X.ci)
                GROUP BY product) AS aux INNER JOIN plaza.product AS p ON p.name = aux.product ORDER BY sold DESC LIMIT 1) LOOP
                
                    FOR W IN (SELECT id_store AS num_store, COUNT(id_store) AS store FROM plaza.bill WHERE client_ci = X.ci GROUP BY num_store
                    ORDER BY store DESC LIMIT 1) LOOP
                                
                        client:= X.full_name;
                        bank_of_preference:= Y.name_bank;
                        category_of_preference:= Z.name_cat;
                        store_of_preference := W.num_store;
                        RETURN NEXT;
                        
                    END LOOP;
                END LOOP; 
            END LOOP;	 	 
        END LOOP;
    END;
    $$ 
    LANGUAGE plpgsql;"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

    switcher = {
        1: one,
        2: two,
        3: three,
        4: four,
        5: five
    }

    func = switcher.get(argument, lambda: outOfBounds())
    return func()


def one(): # 1.- Obtener qué banco, categoría de producto, y tienda prefieren nuestros clientes afiliados.

    query= """	 WITH bank AS (SELECT bank_of_preference, COUNT(bank_of_preference) cuenta FROM plaza.client_preferences() 
	 GROUP BY bank_of_preference ORDER BY cuenta DESC LIMIT 1),

	 cat AS (SELECT category_of_preference, COUNT(category_of_preference) cuenta FROM plaza.client_preferences() 
	 GROUP BY category_of_preference ORDER BY cuenta DESC LIMIT 1),
	 
	 store AS(SELECT store_of_preference, COUNT(store_of_preference) cuenta FROM plaza.client_preferences() 
	 GROUP BY store_of_preference ORDER BY cuenta DESC LIMIT 1)

	 SELECT bank.bank_of_preference, cat.category_of_preference, store.store_of_preference FROM bank, cat, store"""
    return print(select(query))

def two(): # 2.- Obtener el banco que prefieren nuestros afiliados junto con la cantidad de clientes cuya preferencia es ese banco.
    query="""SELECT bank_of_preference, COUNT(bank_of_preference) AS nr_clients FROM plaza.client_preferences() 
    GROUP BY bank_of_preference ORDER BY nr_clients DESC LIMIT 1;"""
    return print(select(query))

def three():# 3.- Obtener la categoría que prefieren nuestros afiliados junto con la cantidad de clientes cuya preferencia es esa categoría.
    query="""SELECT category_of_preference, COUNT(category_of_preference) AS nr_clients FROM plaza.client_preferences() 
    GROUP BY category_of_preference ORDER BY nr_clients DESC LIMIT 1;"""
    return print(select(query))

def four(): # 4.- Obtener la tienda que prefieren nuestros afiliados junto con la cantidad de clientes cuya preferencia es esa tienda.
    query="""SELECT store_of_preference, COUNT(store_of_preference) AS nr_clients FROM plaza.client_preferences() 
    GROUP BY store_of_preference ORDER BY nr_clients DESC LIMIT 1;"""
    return print(select(query))

def five(): #5.- Obtener las preferencias individuales de cada uno de nuestros clientes afiliados.
    query=""" SELECT * FROM plaza.client_preferences();"""
    return print(select(query))

def outOfBounds():
    return print('Invalid entry to method client_preferences')

# UNCOMMENT THE LINE BELOW TO RUN THE FUNCTION. PARAMETER INT FROM 1 TO 5 DEPENDING OF WHAT QUERY IS DESIRED.
#TO ANSWER THE QUESTION PARAMETER = 1
# client_preferences(1)


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

        CREATE OR REPLACE FUNCTION plaza.yesterdayInStock(id_store_ integer, shelf_id_ integer) 
        RETURNS TABLE (shelf_id integer, product_name character varying, date date, hour numeric, average integer) 
        AS $$
        BEGIN
            RETURN QUERY 
                SELECT plaza.in_stock.shelf_id AS shelf_id, plaza.shelf.product_name AS product_name, 
                    datetime::date AS date, CAST(EXTRACT(HOUR FROM datetime) AS NUMERIC) AS hour, 
                CAST(AVG(qty_available) AS INT) AS average
                FROM plaza.in_stock
                INNER JOIN plaza.shelf ON plaza.shelf._id = plaza.in_stock.shelf_id 
                WHERE CURRENT_DATE::date - datetime::date = 1 AND
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

    query=f"SELECT * FROM plaza.yesterdayInStock({id_store}, {shelf_id})"
    print(select(query))


# Con esto se sabrá el promedio por hora de la disponibilidad del producto en el estante ("shelf") que se especifique de la
# tienda ("store") elegida. Las horas que no aparecen es porque se mantuvo igual que el registro anterior.

# En este caso, especificó cuál fue el comportamiento de la disponibilidad por hora en el estante 3 de la tienda 1.

# UNCOMMENT THE LINE BELOW TO RUN THE FUNCTION.
# yesterdayInStock(1,3)
