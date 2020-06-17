import psycopg2

# Connect to de DB
conn = psycopg2.connect(
    host='drona.db.elephantsql.com',
    user ='ftuzkdcj',
    password='7UHxXzyMvKwsIqOa9nnC8frDFsesnn6U',
    database='ftuzkdcj'
)

# query="""
#     INSERT INTO plaza.client (ci, name, last_name)
#     VALUES ('v27654321', 'Victoria', 'Acuna'),
#            ('v27123456', 'Wilfredo', 'Machado'),
#            ('e27654321', 'Romel', 'Clavel')
# ;"""

# query="""
#     INSERT INTO plaza.membership (ci, points)
#     VALUES ('v27654321', 0),
#            ('v27123456', 10)
# ;"""

# query="""
#     INSERT INTO plaza.store (max_people, address)
#     VALUES (10, 'Terrazas del Ávila'),
#            (15, 'Los Naranjos')
# ;"""

# query="""
#     INSERT INTO plaza.product (name, id_store, category)
#     VALUES ('Manzana', 2, 'Frutas'),
#            ('Zucaritas', 1, 'Cereales'),
#            ('Papel Toilet', 2, 'Higiene')

# ;"""

# query="""
#     INSERT INTO plaza.price (product_name, id_store, price, cost, date)
#     VALUES ('Manzana', 2, 500.50, 200, CURRENT_DATE),
#            ('Manzana', 1, 500, 200, '2020-06-15'),
#            ('Manzana', 2, 400, 200, '2020-06-15')

# ;"""

# query="""
#     INSERT INTO plaza.bill (client_ci, id_store, account, datetime, total)
#     VALUES ('e27654321', 1, 'Mercantil', '2020-06-15 02:29:30.186127', 0)
# ;"""

query="""
    INSERT INTO plaza.bill_product (bill_id, product_name, quantity)
    VALUES (4, 'Manzana', 1)
;"""

# query="""
#     DELETE FROM plaza.bill
#     WHERE _id=3
# ;"""




# Close the connection
# conn.close()

cur = conn.cursor()
cur.execute(query)
cur.close()
conn.commit()