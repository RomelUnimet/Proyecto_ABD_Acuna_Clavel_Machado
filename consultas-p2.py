import psycopg2

# Connect to de DB
conn = psycopg2.connect(
    host='ruby.db.elephantsql.com',
    user ='fvhavaif',
    password='THCA_nW8eWwmkuQ4mkobpS0qvZNLEYzE',
    database='fvhavaif'
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
    

    ;"""
    print(select(query))


