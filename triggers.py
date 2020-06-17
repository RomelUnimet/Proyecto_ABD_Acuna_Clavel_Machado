import psycopg2

# Connect to de DB
conn = psycopg2.connect(
    host='drona.db.elephantsql.com',
    user ='ftuzkdcj',
    password='7UHxXzyMvKwsIqOa9nnC8frDFsesnn6U',
    database='ftuzkdcj'
)

# FIRST TRIGGER
# Trigger that adds a point to a member whenever he/she makes a visit.
def createTriggerAddPointWhenVisit():

    query = """
    CREATE OR REPLACE FUNCTION addPointWhenVisit()
    RETURNS trigger AS $$
    BEGIN 
        IF NEW.client_ci IN (SELECT ci FROM plaza.membership WHERE ci=NEW.client_ci)
        THEN
            UPDATE plaza.membership
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
    AFTER INSERT ON plaza.visit
        FOR EACH ROW 
            EXECUTE PROCEDURE addPointWhenVisit()       
    ;"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()
# createTriggerAddPointWhenVisit()




# SECOND TRIGGER
# Trigger that adds points to a member whenever he/she makes a purchase.
def createTriggerAddPointsWhenPurchase():

    query = """
    CREATE OR REPLACE FUNCTION addPointsWhenPurchase()
    RETURNS trigger AS $$
    DECLARE 
        store_ integer := (SELECT id_store FROM plaza.bill WHERE _id = NEW.bill_id);
        date_ date := (SELECT datetime FROM plaza.bill WHERE NEW.bill_id=_id)::date;
        price numeric := (SELECT price FROM plaza.price
                          WHERE product_name = NEW.product_name AND
                          date = date_ AND
                          store_ = id_store
                         );
    BEGIN 
        IF NEW.bill_id IN (SELECT _id FROM plaza.bill, plaza.membership
                           WHERE plaza.bill.client_ci = plaza.membership.ci
                           )
        THEN
            UPDATE plaza.membership
            SET points = CAST( (points+((price*NEW.quantity)*0.1))  AS INT)
            WHERE ci IN (SELECT client_ci FROM plaza.bill, plaza.membership
                        WHERE plaza.bill.client_ci = plaza.membership.ci
                        );
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
    AFTER INSERT ON plaza.bill_product
        FOR EACH ROW 
            EXECUTE PROCEDURE addPointsWhenPurchase()       
    ;"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

createTriggerAddPointsWhenPurchase()


# ADDITIONAL TRIGGERS 
# Trigger that updates the total of a bill whenever he/she adds a product.
def createTriggerUpdateTotal():

    query = """
    CREATE OR REPLACE FUNCTION updateTotal()
    RETURNS trigger AS $$
    DECLARE 
        store_ integer := (SELECT id_store FROM plaza.bill WHERE _id = NEW.bill_id);
        date_ date := (SELECT datetime FROM plaza.bill WHERE NEW.bill_id=_id)::date;
        price numeric := (SELECT price FROM plaza.price
                          WHERE product_name = NEW.product_name AND
                          date = date_ AND
                          store_ = id_store
                         );
    BEGIN 
        UPDATE plaza.bill
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
    AFTER INSERT ON plaza.bill_product
        FOR EACH ROW 
            EXECUTE PROCEDURE updateTotal()       
    ;"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()


createTriggerUpdateTotal()

# query="DROP TRIGGER addPointsWhenPurchase ON plaza.bill_product"
# cur = conn.cursor()
# cur.execute(query)
# cur.close()
# conn.commit()

# query="DROP TRIGGER updateTotal ON plaza.bill_product"
# cur = conn.cursor()
# cur.execute(query)
# cur.close()
# conn.commit()




