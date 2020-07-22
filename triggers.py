import psycopg2

# Connect to de DB
conn = psycopg2.connect(
    host='ruby.db.elephantsql.com',
    user ='fvhavaif',
    password='THCA_nW8eWwmkuQ4mkobpS0qvZNLEYzE',
    database='fvhavaif'
)

# FIRST TRIGGER
# Trigger that adds a point to a member whenever he/she makes a visit.
def createTriggerAddPointWhenVisit():

    query = """
    CREATE OR REPLACE FUNCTION ventas.addPointWhenVisit()
    RETURNS trigger AS $$
    BEGIN 
        IF NEW.client_ci IN (SELECT ci FROM ventas.membership WHERE ci=NEW.client_ci)
        THEN
            UPDATE ventas.membership
            SET points = (points+1)
            WHERE ci = NEW.client_ci;
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    ;"""

    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

    query = """
    CREATE TRIGGER addPointWhenVisit
    AFTER INSERT ON ventas.visit
        FOR EACH ROW 
            EXECUTE PROCEDURE ventas.addPointWhenVisit()       
    ;"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()
createTriggerAddPointWhenVisit()


# SECOND TRIGGER
# Trigger that adds points to a member whenever he/she makes a purchase.

def createTriggerAddPointsWhenPurchase():

    query = """
    CREATE OR REPLACE FUNCTION ventas.addPointsWhenPurchase()
    RETURNS trigger AS $$
    DECLARE 
        date_ date := (
            SELECT MAX(date) FROM ventas.price
            WHERE product_name=NEW.product_name AND id_store=NEW.id_store
        )::date;
        price numeric := (SELECT price FROM ventas.price
                          WHERE product_name = NEW.product_name AND
                          date = date_ AND
                          NEW.id_store = id_store
                         );
        ci_ character varying := (
                                    SELECT client_ci FROM ventas.bill
                                    WHERE _id = NEW.bill_id
                                  );
    BEGIN 
        IF NEW.bill_id IN (SELECT _id FROM ventas.bill, ventas.membership
                           WHERE ventas.bill.client_ci = ventas.membership.ci
                           )
        THEN
            UPDATE ventas.membership
            SET points = CAST( (points+((price*NEW.quantity)*0.1))  AS INT)
            WHERE ci = ci_;
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    ;"""

    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

    query = """
    CREATE TRIGGER addPointsWhenPurchase
    AFTER INSERT ON ventas.bill_product
        FOR EACH ROW 
            EXECUTE PROCEDURE ventas.addPointsWhenPurchase()       
    ;"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()


createTriggerAddPointsWhenPurchase()


# THIRD TRIGGER
# Trigger that makes a client a member when she/he has made more than four visits.
def createTriggerAddMembership():

    query = """
    CREATE OR REPLACE FUNCTION ventas.addMembership()
    RETURNS trigger AS $$
    DECLARE
        purchases numeric := (SELECT COUNT(client_ci) FROM ventas.bill WHERE client_ci = NEW.client_ci);
    BEGIN 
        IF NEW.client_ci NOT IN (
            SELECT ci FROM ventas.membership
            WHERE ci = NEW.client_ci 
        )
        THEN
            IF (purchases>4)
            THEN
                INSERT INTO ventas.membership (ci, points)
                VALUES (NEW.client_ci, 0);
            END IF;
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    ;"""

    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

    query = """
    CREATE TRIGGER addMembership
    AFTER INSERT ON ventas.bill
        FOR EACH ROW 
            EXECUTE PROCEDURE ventas.addMembership()       
    ;"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

createTriggerAddMembership()


# ADDITIONAL TRIGGERS 
# Trigger that updates the total of a bill whenever he/she adds a product.
def createTriggerUpdateTotal():

    query = """
    CREATE OR REPLACE FUNCTION ventas.updateTotal()
    RETURNS trigger AS $$
    DECLARE 
        date_ date := (
            SELECT MAX(date) FROM ventas.price
            WHERE product_name=NEW.product_name AND id_store=NEW.id_store
        )::date;
        price numeric := (SELECT price FROM ventas.price
                          WHERE product_name = NEW.product_name AND
                          date = date_ AND
                          NEW.id_store = id_store
                         );
    BEGIN 
        UPDATE ventas.bill
        SET total = (total+(price*NEW.quantity))
        WHERE NEW.bill_id = _id;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    ;"""

    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

    query = """
    CREATE TRIGGER updateTotal
    AFTER INSERT ON ventas.bill_product
        FOR EACH ROW 
            EXECUTE PROCEDURE ventas.updateTotal()       
    ;"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()


createTriggerUpdateTotal()











