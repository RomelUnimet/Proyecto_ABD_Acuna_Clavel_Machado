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
#     VALUES ('Helado de fresa', 1, 'Congelados'),
#            ('Helado de fresa', 2, 'Congelados'),
#            ('Tequeños', 1, 'Congelados'),
#            ('Tequeños', 2, 'Congelados'),
#            ('Helado de chocolate', 1, 'Congelados'),
#            ('Helado de chocolate', 2, 'Congelados')

# ;"""

# query="""
#     INSERT INTO plaza.price (product_name, id_store, price, cost, date)
#     VALUES ('Manzana', 1, 280, 100, '2020-06-12')

#  ;"""

# query="""
#     INSERT INTO plaza.bill (client_ci, id_store, account, datetime, total)
#     VALUES  ('e27111000', 1, 'Provincial', '2020-06-17 10:30.396128', 0),
#             ('v27000222', 1, 'Mercantil', '2020-06-18 10:30.396128', 0),
#             ('e27000111', 2, 'Banesco', '2020-06-18 10:30.396128', 0),
#             ('v27000000', 2, 'Mercantil', '2020-06-18 11:30.396128', 0)

            

# ;"""


# query="""
#     INSERT INTO plaza.visit (client_ci, id_store, datetime)
#     VALUES ('e27111000', 1, '2020-06-17 9:30.396128'),
#            ('v27000222', 1, '2020-06-18 9:30.396128'),
#            ('e27000111', 2, '2020-06-18 9:30.396128'),
#            ('v27000000', 2, '2020-06-18 11:30.396128')
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
#     INSERT INTO plaza.shelf (id_store, capacity, product_name, min_temperature)
#     VALUES 
#            (1, 50, 'Helado de fresa', -19),
#            (2, 25, 'Helado de fresa', -19),
#            (1, 30, 'Helado de chocolate', -19),
#            (2, 30, 'Helado de chocolate', -19),
#            (1, 40, 'Tequeños', -19),
#            (2, 80, 'Tequeños', -19)
# ;"""

# query="""
#     INSERT INTO plaza.in_stock (shelf_id, id_store, datetime, qty_available)
#     VALUES (3, 1, '2020-06-10 10:59.396128', 40),
#            (3, 1, '2020-06-10 10:10.396128', 47)


           
# ;"""

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
# Close the connection
# conn.close()
query="""
    SELECT MAX(date) FROM plaza.price
    WHERE product_name='Manzana' AND id_store='2'
"""
print(select(query))
# cur = conn.cursor()
# cur.execute(query)
# cur.close()
# conn.commit()