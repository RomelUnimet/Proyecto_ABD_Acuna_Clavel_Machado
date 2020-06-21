import psycopg2

# Connect to de DB
conn = psycopg2.connect(
    host='drona.db.elephantsql.com',
    user ='ftuzkdcj',
    password='7UHxXzyMvKwsIqOa9nnC8frDFsesnn6U',
    database='ftuzkdcj'
)

query="""
    INSERT INTO plaza.client (ci, name, last_name)
    VALUES ('v25000000', 'Nicol', 'Leal'),
           ('v25000111', 'Massimo', 'Tassinari'),
           ('e25111000', 'Leonardo', 'González'),
           ('e25222000', 'Patricia', 'Borrero'),
           ('v25000222', 'Leonardo', 'Jiménez'),
           ('e25000000', 'Diego', 'Fernandes'),
           ('e25000112', 'Gianmarco', 'De Pacificis'),
           ('v26000000', 'Nicole', 'Brito'),
           ('v26000111', 'Giselle', 'Ferreira'),
           ('e26111000', 'Erika', 'Jiménez'),
           ('e26222000', 'Sofía', 'Machado'),
           ('v26000222', 'Fabiana', 'Acuña'),
           ('e26000000', 'Graciela', 'Ramírez'),
           ('e28000111', 'Francesco', 'Donnarumma'),
           ('v28000000', 'Carlos', 'Piñerua'),
           ('v28000111', 'Carlota', 'González'),
           ('e28111000', 'Mercedes', 'Grau'),
           ('e28222000', 'Sofía', 'González'),
           ('v28000222', 'Sara', 'Garroni'),
           ('e28000000', 'Félix', 'Marin'),
           ('e29000111', 'Vanessa', 'Marin')


;"""

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

query="""
    INSERT INTO plaza.in_stock (id_store, qty_available, shelf_id, datetime)
    VALUES (1, 50, 64, '2020-06-18 10:30.396128'),
           (1, 50, 65, '2020-06-18 10:30.396128'),
           (2, 50, 66, '2020-06-18 10:30.396128'),
           (1, 100, 67, '2020-06-18 10:30.396128'),
           (2, 150, 68, '2020-06-18 10:30.396128'),
           (1, 200, 69, '2020-06-18 10:30.396128'),
           (1, 200, 70, '2020-06-18 10:30.396128'),
           (2, 150, 71, '2020-06-18 10:30.396128'),
           (1, 100, 72, '2020-06-18 10:30.396128'),
           (2, 200, 73, '2020-06-18 10:30.396128'),
           (1, 70, 74, '2020-06-18 10:30.396128'),
           (2, 80, 75, '2020-06-18 11:30.396128')
           
;"""



# query="""
#     INSERT INTO plaza.price (product_name, id_store, price, cost, date)
#     VALUES 
#            ('Arvejas', 1, 200, 100, '2020-06-18'),
#            ('Mayonesa', 2, 230, 100, '2020-06-18'),
#            ('Pan Bimbo', 2, 250, 100, '2020-06-18')

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
#            (34, 'Arvejas', 2, 1),
#            (35, 'Mayonesa', 7, 2),
#            (36, 'Pan Bimbo', 2, 2)
# ;"""

# query="""
#     DELETE FROM plaza.visit
#     WHERE client_ci='e27000111'
# ;"""

# query="""
#     DELETE FROM plaza.bill
#     WHERE client_ci='v27654321'
# ;"""

query="""
    DELETE FROM plaza.price
    WHERE date='2020'
;"""

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
# query="""
#     SELECT _id FROM plaza.bill, plaza.membership
#                            WHERE plaza.bill.client_ci = plaza.membership.ci
# """
# print(select(query))
cur = conn.cursor()
cur.execute(query)
cur.close()
conn.commit()