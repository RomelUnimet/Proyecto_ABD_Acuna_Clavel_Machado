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
#     VALUES ('v27654322', 'Paola', 'Sollecito'),
#            ('v27123457', 'Andres', 'Rodriguez'),
#            ('e27654325', 'Christian', 'Guillen')
# ;"""

# query="""
#     INSERT INTO plaza.membership (ci, points)
#     VALUES ('v27654322', 45)
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

query="""
    INSERT INTO plaza.bill (client_ci, id_store, account, datetime, total)
    VALUES ('e27654321', 1, 'Mercantil', '2020-06-15 02:29:30.396128', 200),
            ('e27654321', 2, 'Mercantil', '2020-06-15 02:29:30.396128', 200),
            ('v27123457', 2, 'Mercantil', '2020-06-15 02:29:30.396128', 100),
            ('v27123457', 2, 'Mercantil', '2020-06-15 03:29:30.396128', 50),
            ('e27654325', 2, 'Mercantil', '2020-06-15 03:29:30.396128', 50),
            ('v27654321', 2, 'Mercantil', '2020-06-15 03:29:30.396128', 500),
            ('v27123456', 1, 'Mercantil', '2020-06-15 05:29:30.396128', 75),
            ('v27654322', 1, 'Mercantil', '2020-06-15 05:29:30.396128', 50)

;"""

# query="""
#     INSERT INTO plaza.bill_product (bill_id, product_name, quantity)
#     VALUES (4, 'Manzana', 1)
# ;"""

# query="""
#     DELETE FROM plaza.bill
#     WHERE _id=3
# ;"""

# query="""
#     DELETE FROM plaza.bill
#     WHERE client_ci='v27654321'
# ;"""

# query="""
#     DELETE FROM plaza.bill_product
#     WHERE bill_id=1 OR bill_id=2
# ;"""





# Close the connection
# conn.close()

cur = conn.cursor()
cur.execute(query)
cur.close()
conn.commit()