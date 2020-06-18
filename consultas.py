# PLEASE GO TO THE END OF THE FILE AND YOU'LL BE ABLE TO RUN THE QUERIES YOU WANT.


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


# FIRST QUERY
# 1.1 Query that finds the clients who have spent more money.

def findTopClientsMoney():
    query="""
    WITH data AS (
        WITH top3MemberStore1 AS (
            SELECT ci, SUM(plaza.bill.total) AS total, 'Cliente miembro' AS membership, 'Tienda 1' AS store FROM plaza.client
            INNER JOIN plaza.bill ON plaza.bill.client_ci = ci
            WHERE id_store = 1 AND ci IN (
                SELECT plaza.membership.ci FROM plaza.membership
            )
            GROUP BY ci
            ORDER BY total DESC
            LIMIT 3
        ),
        top3MemberStore2 AS (
            SELECT ci, SUM(plaza.bill.total) AS total, 'Cliente miembro' AS membership, 'Tienda 2' AS store FROM plaza.client
            INNER JOIN plaza.bill ON plaza.bill.client_ci = ci
            WHERE id_store = 2 AND ci IN (
                SELECT plaza.membership.ci FROM plaza.membership
            )
            GROUP BY ci
            ORDER BY total DESC
            LIMIT 3
        ),
        top3UnknowStore1 AS (
            SELECT ci, SUM(plaza.bill.total) AS total, 'Cliente desconocido' AS membership, 'Tienda 1' AS store FROM plaza.client
            INNER JOIN plaza.bill ON plaza.bill.client_ci = ci
            WHERE id_store = 1 AND ci NOT IN (
                SELECT plaza.membership.ci FROM plaza.membership
            )
            GROUP BY ci
            ORDER BY total DESC
            LIMIT 3
        ),
        top3UnknowStore2 AS (
            SELECT ci, SUM(plaza.bill.total) AS total, 'Cliente desconocido' AS membership, 'Tienda 2' AS store FROM plaza.client
            INNER JOIN plaza.bill ON plaza.bill.client_ci = ci
            WHERE id_store = 2 AND ci NOT IN (
                SELECT plaza.membership.ci FROM plaza.membership
            )
            GROUP BY ci
            ORDER BY total DESC
            LIMIT 3
        )

        SELECT * FROM top3MemberStore1
        UNION
        SELECT * FROM top3MemberStore2
        UNION 
        SELECT * FROM top3UnknowStore1
        UNION 
        SELECT * FROM top3UnknowStore2
    )
    SELECT * FROM data
    ORDER BY store, membership, total DESC

    ;"""
    print(select(query))




# FIRST QUERY
# 1.2 Query that finds the clients who have made more purchases.

def findTopClientsPurchases():
    query="""
    WITH data AS (
        WITH top3MemberStore1 AS (
            SELECT ci, COUNT(plaza.bill.client_ci) AS total_purchases, 'Cliente miembro' AS membership, 'Tienda 1' AS store FROM plaza.client
            INNER JOIN plaza.bill ON plaza.bill.client_ci = ci
            WHERE id_store = 1 AND ci IN (
                SELECT plaza.membership.ci FROM plaza.membership
            )
            GROUP BY ci
            ORDER BY total_purchases DESC
            LIMIT 3
        ),
        top3MemberStore2 AS (
            SELECT ci, COUNT(plaza.bill.client_ci) AS total_purchases, 'Cliente miembro' AS membership, 'Tienda 2' AS store FROM plaza.client
            INNER JOIN plaza.bill ON plaza.bill.client_ci = ci
            WHERE id_store = 2 AND ci IN (
                SELECT plaza.membership.ci FROM plaza.membership
            )
            GROUP BY ci
            ORDER BY total_purchases DESC
            LIMIT 3
        ),
        top3UnknowStore1 AS (
            SELECT ci, COUNT(plaza.bill.client_ci) AS total_purchases, 'Cliente desconocido' AS membership, 'Tienda 1' AS store FROM plaza.client
            INNER JOIN plaza.bill ON plaza.bill.client_ci = ci
            WHERE id_store = 1 AND ci NOT IN (
                SELECT plaza.membership.ci FROM plaza.membership
            )
            GROUP BY ci
            ORDER BY total_purchases DESC
            LIMIT 3
        ),
        top3UnknowStore2 AS (
            SELECT ci, COUNT(plaza.bill.client_ci) AS total_purchases, 'Cliente desconocido' AS membership, 'Tienda 2' AS store FROM plaza.client
            INNER JOIN plaza.bill ON plaza.bill.client_ci = ci
            WHERE id_store = 2 AND ci NOT IN (
                SELECT plaza.membership.ci FROM plaza.membership
            )
            GROUP BY ci
            ORDER BY total_purchases DESC
            LIMIT 3
        )

        SELECT * FROM top3MemberStore1
        UNION
        SELECT * FROM top3MemberStore2
        UNION 
        SELECT * FROM top3UnknowStore1
        UNION 
        SELECT * FROM top3UnknowStore2
    )
    SELECT * FROM data
    ORDER BY store, membership, total_purchases DESC
    ;"""
    print(select(query))



# SECOND QUERY
# Query that finds the least popular category.

def findLeastPopularCategory():
    query = """
        WITH data AS (
            WITH leastPopularStore1 AS (
                SELECT plaza.product.category AS category, SUM(plaza.bill_product.quantity) AS sales, plaza.bill_product.id_store 
                AS store FROM plaza.bill_product, plaza.product
                WHERE plaza.bill_product.product_name = plaza.product.name AND
                    plaza.bill_product.id_store = plaza.product.id_store AND
                    plaza.bill_product.id_store = 1
                GROUP BY category, store
                ORDER BY sales ASC
                LIMIT 1
            ),
            leastPopularStore2 AS (
                SELECT plaza.product.category AS category, SUM(plaza.bill_product.quantity) AS sales, plaza.bill_product.id_store 
                AS store FROM plaza.bill_product, plaza.product
                WHERE plaza.bill_product.product_name = plaza.product.name AND
                    plaza.bill_product.id_store = plaza.product.id_store AND
                    plaza.bill_product.id_store = 2
                GROUP BY category, store
                ORDER BY sales ASC
                LIMIT 1
            )
            SELECT * FROM leastPopularStore1
            UNION
            SELECT * FROM leastPopularStore2
        )
        SELECT * FROM data
        ORDER BY store ASC
    ;"""
    print(select(query))




# THIRD QUERY
# Query that finds the most popular products.

def findMostPopularProducts():
    query = """
        WITH data AS (
            WITH mostPopularStore1 AS (
                SELECT plaza.product.name AS name, SUM(plaza.bill_product.quantity) AS sales, plaza.bill_product.id_store 
                AS store FROM plaza.bill_product, plaza.product
                WHERE plaza.bill_product.product_name = plaza.product.name AND
                    plaza.bill_product.id_store = plaza.product.id_store AND
                    plaza.bill_product.id_store = 1
                GROUP BY name, store
                ORDER BY sales DESC
                LIMIT 5
            ),
            mostPopularStore2 AS (
                SELECT plaza.product.name AS name, SUM(plaza.bill_product.quantity) AS sales, plaza.bill_product.id_store 
                AS store FROM plaza.bill_product, plaza.product
                WHERE plaza.bill_product.product_name = plaza.product.name AND
                    plaza.bill_product.id_store = plaza.product.id_store AND
                    plaza.bill_product.id_store = 2
                GROUP BY name, store
                ORDER BY sales DESC
                LIMIT 5
            )
            SELECT * FROM mostPopularStore1
            UNION
            SELECT * FROM mostPopularStore2
        )
        SELECT * FROM data
        ORDER BY store ASC, sales DESC
    ;"""
    print(select(query))



# FOURTH QUERY
# 4.1 Query that finds the clients who have made at least a purchase only in one of the stores during the last week.
# The query also gives information about what store was visited.
def findClientsWhoHaveMadePurchasesInOneStore():
    query="""

        WITH data AS (
    
            WITH store1 AS (

                SELECT client_ci AS client, id_store AS storeVisited FROM plaza.bill
                WHERE DATE_PART('day', current_date::timestamp - plaza.bill.datetime::timestamp) <= 7 AND
                    id_store=1 AND
                    client_ci NOT IN (
                        SELECT client_ci AS client FROM plaza.bill
                            WHERE DATE_PART('day', current_date::timestamp - plaza.bill.datetime::timestamp) <= 7 AND
                                id_store=2
                    )
                GROUP BY client, storeVisited
                ORDER BY client

            ),

            store2 AS (

                SELECT client_ci AS client, id_store AS storeVisited FROM plaza.bill
                WHERE DATE_PART('day', current_date::timestamp - plaza.bill.datetime::timestamp) <= 7 AND
                    id_store=2 AND
                    client_ci NOT IN (
                        SELECT client_ci AS client FROM plaza.bill
                            WHERE DATE_PART('day', current_date::timestamp - plaza.bill.datetime::timestamp) <= 7 AND
                                id_store=1
                    )
                GROUP BY client, storeVisited
                ORDER BY client

            )
            SELECT * FROM store1
            UNION
            SELECT * FROM store2
            
        )
        SELECT * FROM data
        ORDER BY storeVisited ASC, client

    ;"""
    print(select(query))



# FOURTH QUERY
# 4.2 Query that finds the clients who have made at least a purchase in both stores during the last week.
def findClientsWhoHaveMadePurchasesInTwoStores():
    query="""

        WITH data AS (
            WITH stores AS (
                SELECT client_ci AS client, id_store AS stores FROM plaza.bill
                WHERE DATE_PART('day', current_date::timestamp - plaza.bill.datetime::timestamp) <= 7
                GROUP BY client, id_store
                ORDER BY client, id_store ASC
            )
            SELECT client, COUNT(stores) AS qtyOfStores FROM stores
            GROUP BY client
        ) 
        SELECT client FROM data
        WHERE qtyOfStores=2
        ORDER BY client

    ;"""
    print(select(query))




# FIFTH QUERY
# Query that finds the average of restock per day of every category and shows a message.

def getRestockInformation():
    query= """

        WITH message AS (
            WITH data AS (

                SELECT plaza.product.category AS category, COUNT(plaza.restock.shelf_id) AS qtyRestock, plaza.restock.datetime::date AS date
                FROM plaza.restock
                INNER JOIN plaza.shelf ON plaza.shelf._id = plaza.restock.shelf_id
                INNER JOIN plaza.product ON plaza.product.name = plaza.shelf.product_name
                WHERE plaza.shelf.id_store = plaza.product.id_store
                GROUP BY category, date
                ORDER BY date, category

            )
            SELECT category, CAST(AVG(qtyRestock) AS INT) AS restockPerDay
            FROM data
            GROUP BY category
        )
        SELECT category, restockPerDay, CASE
            WHEN restockPerDay>2 THEN 'Deberías revisar la capacidad de estos estantes. Te recomendamos aumentarla.'
            ELSE
                'La capacidad de estos estantes es adecuada.'
            END AS message
        FROM message
        ORDER BY restockPerDay DESC, category

    ;"""
    print(select(query))



#SIXTH QUERY
# Query that finds the clients who have paid with two types of account during the last week.
def findClientsWithMoreThanTwoAccounts():
    query="""

        WITH accounts AS (
    
            WITH qty AS (
                SELECT client_ci AS client, account FROM plaza.bill
                WHERE DATE_PART('day', current_date::timestamp - plaza.bill.datetime::timestamp) <= 7
                GROUP BY client, account
                ORDER BY client, account
            )
            SELECT client, COUNT(account) AS accounts FROM qty 
            GROUP BY client
            ORDER BY accounts DESC 
        ), 
        
        membership AS (
        
            SELECT ci AS client, CASE 
                        WHEN plaza.client.ci IN (SELECT ci FROM plaza.membership) THEN 'Member'
                        ELSE 'Unknown client'
                    END AS membership
            FROM plaza.client
            
        )
        
        SELECT accounts.client, accounts.accounts AS qtyOfAccounts, membership.membership FROM accounts, membership
        WHERE accounts = 2 AND
            membership.client=accounts.client

    ;"""
    print(select(query))




def runQueries():
    # Please comment the queries you don't want to run.

    # FIRST QUERY
    # 1.1 Query that finds the clients who have spent more money.
    findTopClientsMoney()

    # FIRST QUERY
    # 1.2 Query that finds the clients who have made more purchases.
    findTopClientsPurchases()

    # SECOND QUERY
    # Query that finds the least popular category.
    findLeastPopularCategory()

    # THIRD QUERY
    # Query that finds the most popular products.
    findMostPopularProducts()

    # FOURTH QUERY
    # 4.1 Query that finds the clients who have made at least a purchase only in one of the stores during the last week.
    # The query also gives information about what store was visited.
    findClientsWhoHaveMadePurchasesInOneStore()

    # FOURTH QUERY
    # 4.2 Query that finds the clients who have made at least a purchase in both stores during the last week.
    findClientsWhoHaveMadePurchasesInTwoStores()

    # FIFTH QUERY
    # Query that finds the average of restock per day of every category and shows a message.
    getRestockInformation()

    #SIXTH QUERY
    # Query that finds the clients who have paid with two types of account during the last week.
    findClientsWithMoreThanTwoAccounts()


runQueries()