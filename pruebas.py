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
#     VALUES ('v27000000', 'Victoria', 'Acuna'),
#            ('v27000111', 'Wilfredo', 'Machado'),
#            ('e27111000', 'Romel', 'Clavel'),
#            ('e27222000', 'Paola', 'Sollecito'),
#            ('v27000222', 'Gianluca', 'Di Bella'),
#            ('e27000000', 'Valeria', 'Trotta'),
#            ('e27000111', 'Valeska', 'Silva')

# ;"""

# query="""
#     INSERT INTO plaza.membership (ci, points)
#     VALUES ('v27000000', 200),
#            ('e27111000', 50),
#            ('v27000222', 100)
# ;"""

# query="""
#     INSERT INTO plaza.store (max_people, address, opening, closing)
#     VALUES (10, 'Terrazas del Ávila', '9:00:00', '20:00:00'),
#            (15, 'Los Naranjos', '9:00:00', '20:00:00')
# ;"""

# query="""
#     INSERT INTO plaza.product (name, id_store, category)
#     VALUES ('Manzana', 1, 'Frutas'),
#            ('Zucaritas', 2, 'Cereales'),
#            ('Papel Toilet', 1, 'Higiene'),
#            ('Manzana', 2, 'Frutas'),
#            ('Pera', 1, 'Frutas'),
#            ('Papel Toilet', 2, 'Higiene')

# ;"""

# query="""
#     INSERT INTO plaza.price (product_name, id_store, price, cost, date)
#     VALUES ('Manzana', 1, 280, 100, '2020-06-12')

#  ;"""

# query="""
#     INSERT INTO plaza.bill (client_ci, id_store, account, datetime, total)
#     VALUES  ('v27000000', 1, 'Provincial', '2020-06-12 10:30.396128', 0)
            

# ;"""


# query="""
#     INSERT INTO plaza.visit (client_ci, id_store, datetime)
#     VALUES ('e27000111', 1, '2020-06-17 9:30.396128')
# ;"""

# query="""
#     INSERT INTO plaza.bill_product (bill_id, product_name, quantity, id_store)
#     VALUES 
#            (15, 'Manzana', 2, 1)
# ;"""

# query="""
#     DELETE FROM plaza.visit
#     WHERE client_ci='e27000111'
# ;"""

# query="""
#     DELETE FROM plaza.bill
#     WHERE client_ci='v27654321'
# ;"""

# query="""
#     DELETE FROM plaza.bill_product
#     WHERE bill_id=8
# ;"""

# query="""
#     DROP TABLE plaza.bill_product, plaza.temperature, plaza.in_stock, plaza.restock, plaza.visit,
#     plaza.shelf, plaza.bill, plaza.membership, plaza.client, plaza.store, plaza.price, plaza.product
# ;"""


# query="""
#     INSERT INTO plaza.shelf (id_store, capacity, product_name)
#     VALUES 
#            (1, 50, 'Manzana'),
#            (2, 25, 'Manzana'),
#            (1, 30, 'Pera'),
#            (1, 100, 'Papel Toilet'),
#            (2, 40, 'Zucaritas'),
#            (2, 80, 'Papel Toilet')
# ;"""

query="""
    INSERT INTO plaza.in_stock (shelf_id, id_store, datetime, qty_available)
    VALUES (3, 1, '2020-06-10 10:59.396128', 40),
           (3, 1, '2020-06-10 10:10.396128', 47)


           
;"""


# Close the connection
# conn.close()

cur = conn.cursor()
cur.execute(query)
cur.close()
conn.commit()