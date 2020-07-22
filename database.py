import psycopg2


def createTablesVentas():
    
    commands = (
        """
        CREATE TABLE ventas.client (
            ci VARCHAR(20) PRIMARY KEY,
            name VARCHAR(25) NOT NULL,
            last_name VARCHAR(25) NOT NULL,
            female BOOLEAN NOT NULL
        )
        """, 
        """
        CREATE TABLE ventas.membership (
            ci VARCHAR(20) PRIMARY KEY,
            points INTEGER,
            FOREIGN KEY (ci)
                REFERENCES ventas.client (ci)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE ventas.store (
            _id SERIAL PRIMARY KEY,
            max_people INTEGER NOT NULL, 
            address VARCHAR(50) NOT NULL,
            opening time NOT NULL,
            closing time NOT NULL
        )
        """,
        """
        CREATE TABLE ventas.product (
            name VARCHAR(30),
            id_store INTEGER, 
            category VARCHAR(20) NOT NULL, 
            PRIMARY KEY (id_store, name),
            FOREIGN KEY (id_store)
                REFERENCES ventas.store (_id)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE ventas.price ( 
            date DATE,
            id_store INTEGER,
            product_name VARCHAR(30),
            price NUMERIC(32, 2) NOT NULL, 
            cost NUMERIC(32, 2) NOT NULL,
            PRIMARY KEY (date, id_store, product_name),
            FOREIGN KEY (id_store, product_name)
                REFERENCES ventas.product (id_store, name)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE ventas.visit (
            client_ci VARCHAR(30),
            id_store INTEGER, 
            datetime TIMESTAMP, 
            PRIMARY KEY (datetime, id_store, client_ci),
            FOREIGN KEY (id_store)
                REFERENCES ventas.store (_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (client_ci)
                REFERENCES ventas.client (ci)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE ventas.bill (
            _id SERIAL PRIMARY KEY, 
            id_store INTEGER,
            client_ci VARCHAR(20) NOT NULL,
            account VARCHAR(15) NOT NULL, 
            datetime TIMESTAMP NOT NULL,
            total NUMERIC(32,2) NOT NULL,
            FOREIGN KEY (id_store)
                REFERENCES ventas.store (_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (client_ci)
                REFERENCES ventas.client (ci)
                ON UPDATE CASCADE
            
        )
        """,
        """
        CREATE TABLE ventas.bill_product (
            bill_id INTEGER, 
            product_name VARCHAR(30),
            id_store INTEGER,
            quantity INTEGER NOT NULL,
            PRIMARY KEY (bill_id, product_name),
            FOREIGN KEY (bill_id)
                REFERENCES ventas.bill (_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (id_store, product_name)
                REFERENCES ventas.product (id_store, name)
                ON UPDATE CASCADE
            
        )
        """
        
    )
    # Connect to de DB
    conn = psycopg2.connect(
        host='ruby.db.elephantsql.com',
        user ='fvhavaif',
        password='THCA_nW8eWwmkuQ4mkobpS0qvZNLEYzE',
        database='fvhavaif'
    )
    # Cursor
    cur = conn.cursor()
    
    for command in commands:
        cur.execute(command)
    cur.close()
    conn.commit()


#createTablesVentas() 

# Close the connection
# conn.close()

def createTablesInventario():

    commands = (
        """
        CREATE TABLE inventario.store (
            _id SERIAL PRIMARY KEY,
            address VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE inventario.product (
            name VARCHAR(30),
            id_store INTEGER, 
            category VARCHAR(20) NOT NULL, 
            PRIMARY KEY (id_store, name),
            FOREIGN KEY (id_store)
                REFERENCES inventario.store (_id)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE inventario.shelf (
            id_store INTEGER,
            _id SERIAL,
            product_name VARCHAR(30) NOT NULL, 
            capacity INTEGER NOT NULL, 
            min_temperature NUMERIC(5,2),
            PRIMARY KEY (id_store, _id),
            FOREIGN KEY (product_name, id_store)
                REFERENCES inventario.product (name, id_store)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE inventario.in_stock (
            shelf_id INTEGER,
            id_store INTEGER,
            datetime TIMESTAMP, 
            qty_available INTEGER NOT NULL,
            PRIMARY KEY (datetime, id_store, shelf_id),
            FOREIGN KEY (id_store, shelf_id)
                REFERENCES inventario.shelf (id_store, _id)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE inventario.restock (
            shelf_id INTEGER,
            id_store INTEGER,
            datetime TIMESTAMP, 
            PRIMARY KEY (datetime, id_store, shelf_id),
            FOREIGN KEY (id_store, shelf_id)
                REFERENCES inventario.shelf (id_store, _id)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE inventario.temperature (
            shelf_id INTEGER,
            id_store INTEGER,
            datetime TIMESTAMP, 
            temperature NUMERIC(5,2) NOT NULL,
            PRIMARY KEY (datetime, id_store, shelf_id),
            FOREIGN KEY (id_store, shelf_id)
                REFERENCES inventario.shelf (id_store, _id)
                ON UPDATE CASCADE
        )
        """
    )
    # Connect to de DB
    conn = psycopg2.connect(
        host='ruby.db.elephantsql.com',
        user ='fvhavaif',
        password='THCA_nW8eWwmkuQ4mkobpS0qvZNLEYzE',
        database='fvhavaif'
    )
    # Cursor
    cur = conn.cursor()
    
    for command in commands:
        cur.execute(command)
    cur.close()
    conn.commit()

#createTablesInventario()

def createTablesVisit_SS():

    commands = (
        """
        CREATE TABLE visit.store (
            _id INTEGER PRIMARY KEY,
            address VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE visit.date (
            _id VARCHAR(15) PRIMARY KEY,
            week INTEGER NOT NULL,
            month INTEGER NOT NULL,
            year INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE visit.visit (
            id_store INTEGER,
            date_id VARCHAR(15),
            genre VARCHAR(15),
            isMember BOOLEAN NOT NULL,
            total INTEGER NOT NULL,
            PRIMARY KEY (id_store, date_id, genre, isMember),
            FOREIGN KEY (id_store)
                REFERENCES visit.store (_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (date_id)
                REFERENCES visit.date (_id)
                ON UPDATE CASCADE
        )
        """
    )
    # Connect to de DB
    conn = psycopg2.connect(
        host='ruby.db.elephantsql.com',
        user ='fvhavaif',
        password='THCA_nW8eWwmkuQ4mkobpS0qvZNLEYzE',
        database='fvhavaif'
    )
    # Cursor
    cur = conn.cursor()
    
    for command in commands:
        cur.execute(command)
    cur.close()
    conn.commit()

#createTablesVisit_SS()


def createTablesRestock_SS():

    commands = (
        """
        CREATE TABLE restock.shelf (
            _id INTEGER PRIMARY KEY,
            product_name VARCHAR(50) NOT NULL,
            capacity INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE restock.store (
            _id INTEGER PRIMARY KEY,
            address VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE restock.date (
            _id VARCHAR(15) PRIMARY KEY,
            week INTEGER NOT NULL,
            month INTEGER NOT NULL,
            year INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE restock.restock (
            id_store INTEGER,
            date_id VARCHAR(15),
            shelf_id INTEGER,
            times INTEGER NOT NULL,
            PRIMARY KEY (id_store, date_id, shelf_id),
            FOREIGN KEY (id_store)
                REFERENCES restock.store (_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (date_id)
                REFERENCES restock.date (_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (shelf_id)
                REFERENCES restock.shelf (_id)
                ON UPDATE CASCADE
        )
        """,
    )
    # Connect to de DB
    conn = psycopg2.connect(
        host='ruby.db.elephantsql.com',
        user ='fvhavaif',
        password='THCA_nW8eWwmkuQ4mkobpS0qvZNLEYzE',
        database='fvhavaif'
    )
    # Cursor
    cur = conn.cursor()
    
    for command in commands:
        cur.execute(command)
    cur.close()
    conn.commit()

#createTablesRestock_SS()

def createTablesPurchase_SS():

    commands = (
        """
        CREATE TABLE product (
            name VARCHAR(50) PRIMARY KEY,
            category VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE store (
            _id INTEGER PRIMARY KEY,
            address VARCHAR(50) NOT NULL
        )
        """,
        """
        CREATE TABLE date (
            _id VARCHAR(15) PRIMARY KEY,
            week INTEGER NOT NULL,
            month INTEGER NOT NULL,
            year INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE purchase (
            id_store INTEGER,
            date_id VARCHAR(15),
            product_name VARCHAR(50),
            bank VARCHAR(50), 
            total NUMERIC(64,2) NOT NULL,
            quantity INTEGER NOT NULL,
            PRIMARY KEY (id_store, date_id, product_name, bank),
            FOREIGN KEY (id_store)
                REFERENCES store (_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (date_id)
                REFERENCES date (_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (product_name)
                REFERENCES product (name)
                ON UPDATE CASCADE
        )
        """,
    )
    # Connect to de DB
    conn = psycopg2.connect(
        host='ruby.db.elephantsql.com',
        user ='fvhavaif',
        password='THCA_nW8eWwmkuQ4mkobpS0qvZNLEYzE',
        database='fvhavaif'
    )
    # Cursor
    cur = conn.cursor()
    
    for command in commands:
        cur.execute(command)
    cur.close()
    conn.commit()

createTablesPurchase_SS()