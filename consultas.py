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
# 1.1 QUERY THAT FINDS THE CLIENTS WHO HAVE SPENT MORE MONEY.

def topClientsMoney():
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

# topClientsMoney()


# FIRST QUERY
# 1.2 QUERY THAT FINDS THE CLIENTS WHO HAVE MADE MORE PURCHASES.

def topClientsPurchases():
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

# topClientsPurchases()


