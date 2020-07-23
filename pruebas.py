import psycopg2

# Connect to de DB
conn = psycopg2.connect(
    host='ruby.db.elephantsql.com',
    user ='fvhavaif',
    password='THCA_nW8eWwmkuQ4mkobpS0qvZNLEYzE',
    database='fvhavaif'
)

# query="""
#     INSERT INTO visit.store (_id, address)
#     VALUES (1, 'Las Mercedes'),
#            (2, 'Altamira')

# ;"""

# query="""
#     INSERT INTO restock.shelf (_id, product_name, capacity)
#     VALUES (1, 'Manzanas', 100),
#            (2, 'Peras', 100),
#            (3, 'Naranjas', 100),
#            (4, 'Fresas', 50)

# ;"""

# query="""
#     INSERT INTO visit.date (_id, week, month, year)
#     VALUES ('01/01/2020', 1, 1, 2020),
#            ('02/01/2020', 2, 1, 2020),
#            ('03/01/2020', 3, 1, 2020),
#            ('04/01/2020', 4, 1, 2020)

# ;"""

# query="""
#     INSERT INTO visit.date (_id, week, month, year)
#     VALUES ('05/02/2020', 5, 2, 2020),
#            ('06/02/2020', 6, 2, 2020),
#            ('07/02/2020', 7, 2, 2020),
#            ('08/02/2020', 8, 2, 2020)

# ;"""

# query="""
#     INSERT INTO restock.restock (date_id, shelf_id, id_store, times)
#     VALUES ('01/01/2020', 1, 1, 1),
#            ('02/01/2020', 1, 1, 1),
#            ('03/01/2020', 1, 1, 1),
#            ('04/01/2020', 1, 1, 1),
#            ('01/01/2020', 2, 2, 1),
#            ('02/01/2020', 2, 2, 1),
#            ('03/01/2020', 2, 2, 1),
#            ('04/01/2020', 2, 2, 2),
#            ('01/01/2020', 3, 1, 1),
#            ('02/01/2020', 3, 1, 1),
#            ('03/01/2020', 3, 1, 1),
#            ('04/01/2020', 3, 1, 1),
#            ('01/01/2020', 4, 1, 3),
#            ('02/01/2020', 4, 1, 3),
#            ('03/01/2020', 4, 1, 3),
#            ('04/01/2020', 4, 1, 1)

# ;"""


# query="""
#     INSERT INTO restock.restock (date_id, shelf_id, id_store, times)
#     VALUES 
#            ('07/02/2020', 2, 1, 40),
#            ('08/02/2020', 2, 1, 2)

# ;"""

# query="""
#     INSERT INTO product (name, category)
#     VALUES ('Manzanas', 'Frutas'),
#            ('Peras', 'Frutas'),
#            ('Naranjas', 'Frutas'),
#            ('Fresas', 'Frutas')
# ;"""

# query="""
#     INSERT INTO date (_id, week, month, year)
#     VALUES ('01/01/2020', 1, 1, 2020),
#            ('02/01/2020', 2, 1, 2020),
#            ('03/01/2020', 3, 1, 2020),
#            ('04/01/2020', 4, 1, 2020)

# ;"""

# query="""
#     INSERT INTO purchase (date_id, product_name, id_store, bank, total, quantity)
#     VALUES ('01/01/2020', 'Manzanas', 1, 'Mercantil', 1500000, 350),
#            ('02/01/2020', 'Peras', 2, 'Banesco', 1900000, 410),
#            ('02/01/2020', 'Naranjas', 1, 'Provincial', 2500000, 370),
#            ('01/01/2020', 'Fresas', 1, 'Provincial', 500000, 100),
#            ('02/01/2020', 'Fresas', 1, 'Banesco', 500000, 100),
#            ('03/01/2020', 'Fresas', 1, 'Provincial', 500000, 100),
#            ('03/01/2020', 'Fresas', 1, 'Mercantil', 500000, 175)

# ;"""

query="""
    INSERT INTO visit.visit (date_id, id_store, genre, isMember, total)
    VALUES ('01/01/2020', 1, 'male', true, 100),
           ('02/01/2020', 1, 'male', true, 100),
           ('03/01/2020', 1, 'male', false, 70),
           ('01/01/2020', 1, 'female', true, 150),
           ('04/01/2020', 1, 'female', true, 200),
           ('01/01/2020', 1, 'female', false, 80),
           ('05/02/2020', 1, 'male', true, 100),
           ('05/02/2020', 1, 'male', false, 100),
           ('05/02/2020', 1, 'female', false, 100),
           ('05/02/2020', 1, 'female', true, 100)

;"""

cur = conn.cursor()
cur.execute(query)
cur.close()
conn.commit()