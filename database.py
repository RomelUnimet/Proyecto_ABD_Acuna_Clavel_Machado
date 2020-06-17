import psycopg2


def createTables():
    
    commands = (
        """
        CREATE TABLE plaza.client (
            ci VARCHAR(20) PRIMARY KEY,
            name VARCHAR(25) NOT NULL,
            last_name VARCHAR(25) NOT NULL
        )
        """,
        """
        CREATE TABLE plaza.membership (
            ci VARCHAR(20) PRIMARY KEY,
            points INTEGER,
            FOREIGN KEY (ci)
                REFERENCES plaza.client (ci)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE plaza.store (
            _id SERIAL PRIMARY KEY,
            max_people INTEGER NOT NULL, 
            address VARCHAR(50) NOT NULL,
            opening time NOT NULL,
            closing time NOT NULL
        )
        """,
        """
        CREATE TABLE plaza.product (
            name VARCHAR(30),
            id_store INTEGER, 
            category VARCHAR(20) NOT NULL, 
            PRIMARY KEY (id_store, name),
            FOREIGN KEY (id_store)
                REFERENCES plaza.store (_id)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE plaza.price ( 
            date DATE,
            id_store INTEGER,
            product_name VARCHAR(30),
            price NUMERIC(32, 2) NOT NULL, 
            cost NUMERIC(32, 2) NOT NULL,
            PRIMARY KEY (date, id_store, product_name),
            FOREIGN KEY (id_store, product_name)
                REFERENCES plaza.product (id_store, name)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE plaza.shelf (
            id_store INTEGER,
            _id SERIAL,
            product_name VARCHAR(30) NOT NULL, 
            capacity INTEGER NOT NULL, 
            min_temperature NUMERIC(5,2),
            PRIMARY KEY (id_store, _id),
            FOREIGN KEY (product_name, id_store)
                REFERENCES plaza.product (name, id_store)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE plaza.in_stock (
            shelf_id INTEGER,
            id_store INTEGER,
            datetime TIMESTAMP, 
            qty_available INTEGER NOT NULL,
            PRIMARY KEY (datetime, id_store, shelf_id),
            FOREIGN KEY (id_store, shelf_id)
                REFERENCES plaza.shelf (id_store, _id)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE plaza.restock (
            shelf_id INTEGER,
            id_store INTEGER,
            datetime TIMESTAMP, 
            PRIMARY KEY (datetime, id_store, shelf_id),
            FOREIGN KEY (id_store, shelf_id)
                REFERENCES plaza.shelf (id_store, _id)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE plaza.temperature (
            shelf_id INTEGER,
            id_store INTEGER,
            datetime TIMESTAMP, 
            temperature NUMERIC(5,2) NOT NULL,
            PRIMARY KEY (datetime, id_store, shelf_id),
            FOREIGN KEY (id_store, shelf_id)
                REFERENCES plaza.shelf (id_store, _id)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE plaza.visit (
            client_ci VARCHAR(30),
            id_store INTEGER, 
            datetime TIMESTAMP, 
            PRIMARY KEY (datetime, id_store, client_ci),
            FOREIGN KEY (id_store)
                REFERENCES plaza.store (_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (client_ci)
                REFERENCES plaza.client (ci)
                ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE plaza.bill (
            _id SERIAL PRIMARY KEY, 
            id_store INTEGER,
            client_ci VARCHAR(20) NOT NULL,
            account VARCHAR(15) NOT NULL, 
            datetime TIMESTAMP NOT NULL,
            total NUMERIC(32,2),
            FOREIGN KEY (id_store)
                REFERENCES plaza.store (_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (client_ci)
                REFERENCES plaza.client (ci)
                ON UPDATE CASCADE
            
        )
        """,
        """
        CREATE TABLE plaza.bill_product (
            bill_id INTEGER, 
            product_name VARCHAR(30),
            id_store INTEGER,
            quantity INTEGER NOT NULL,
            PRIMARY KEY (bill_id, product_name),
            FOREIGN KEY (bill_id)
                REFERENCES plaza.bill (_id)
                ON UPDATE CASCADE,
            FOREIGN KEY (id_store, product_name)
                REFERENCES plaza.product (id_store, name)
                ON UPDATE CASCADE
            
        )
        """
    )
    # Connect to de DB
    conn = psycopg2.connect(
        host='drona.db.elephantsql.com',
        user ='ftuzkdcj',
        password='7UHxXzyMvKwsIqOa9nnC8frDFsesnn6U',
        database='ftuzkdcj'
    )
    # Cursor
    cur = conn.cursor()
    
    for command in commands:
        cur.execute(command)
    cur.close()
    conn.commit()


createTables() 

# Close the connection
# conn.close()