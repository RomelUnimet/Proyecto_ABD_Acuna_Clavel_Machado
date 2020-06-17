# import ssl
# import sys
import psycopg2
# import pandas as pd
# from sqlalchemy import create_engine

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

# def pretty_select(query):
#     connection_string = 'postgres://{}:{}@{}:5432/{}'.format(user, password, host, dbname)
#     try:
#         engine = create_engine(connection_string)
#         records = pd.read_sql_query(query, engine)
#     except Exception as e:
#         print('Error en el query:', e)
#     else:
#         return records

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

# findTopClientsMoney()


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

# findTopClientsPurchases()


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


# findLeastPopularCategory()

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


# findMostPopularProducts()



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

findClientsWithMoreThanTwoAccounts()


