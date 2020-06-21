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
    CREATE OR REPLACE FUNCTION plaza.addPointWhenVisit()
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
            EXECUTE PROCEDURE plaza.addPointWhenVisit()       
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
    CREATE OR REPLACE FUNCTION plaza.addPointsWhenPurchase()
    RETURNS trigger AS $$
    DECLARE 
        date_ date := (
            SELECT MAX(date) FROM plaza.price
            WHERE product_name=NEW.product_name AND id_store=NEW.id_store
        )::date;
        price numeric := (SELECT price FROM plaza.price
                          WHERE product_name = NEW.product_name AND
                          date = date_ AND
                          NEW.id_store = id_store
                         );
        ci_ character varying := (
                                    SELECT client_ci FROM plaza.bill
                                    WHERE _id = NEW.bill_id
                                  );
    BEGIN 
        IF NEW.bill_id IN (SELECT _id FROM plaza.bill, plaza.membership
                           WHERE plaza.bill.client_ci = plaza.membership.ci
                           )
        THEN
            UPDATE plaza.membership
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
    AFTER INSERT ON plaza.bill_product
        FOR EACH ROW 
            EXECUTE PROCEDURE plaza.addPointsWhenPurchase()       
    ;"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

# def createTriggerAddPointsWhenPurchase():

#     query = """
#     CREATE OR REPLACE FUNCTION plaza.addPointsWhenPurchase()
#     RETURNS trigger AS $$
#     DECLARE 
#         date_ date := (SELECT datetime FROM plaza.bill WHERE NEW.bill_id=_id)::date;
#         price numeric := (SELECT price FROM plaza.price
#                           WHERE product_name = NEW.product_name AND
#                           date = date_ AND
#                           NEW.id_store = id_store
#                          );
#     BEGIN 
#         IF NEW.bill_id IN (SELECT _id FROM plaza.bill, plaza.membership
#                            WHERE plaza.bill.client_ci = plaza.membership.ci
#                            )
#         THEN
#             UPDATE plaza.membership
#             SET points = CAST( (points+((price*NEW.quantity)*0.1))  AS INT)
#             WHERE ci IN (SELECT client_ci FROM plaza.bill, plaza.membership
#                         WHERE plaza.bill.client_ci = plaza.membership.ci
#                         );
#         END IF;
#         RETURN NEW;
#     END;
#     $$ LANGUAGE plpgsql;
#     ;"""

#     cur = conn.cursor()
#     cur.execute(query)
#     cur.close()
#     conn.commit()

#     query = """
#     CREATE TRIGGER addPointsWhenPurchase
#     AFTER INSERT ON plaza.bill_product
#         FOR EACH ROW 
#             EXECUTE PROCEDURE plaza.addPointsWhenPurchase()       
#     ;"""
#     cur = conn.cursor()
#     cur.execute(query)
#     cur.close()
#     conn.commit()

createTriggerAddPointsWhenPurchase()


# THIRD TRIGGER
# Trigger that makes a client a member when she/he has made more than four visits.
def createTriggerAddMembership():

    query = """
    CREATE OR REPLACE FUNCTION plaza.addMembership()
    RETURNS trigger AS $$
    DECLARE
        purchases numeric := (SELECT COUNT(client_ci) FROM plaza.bill WHERE client_ci = NEW.client_ci);
    BEGIN 
        IF NEW.client_ci NOT IN (
            SELECT ci FROM plaza.membership
            WHERE ci = NEW.client_ci 
        )
        THEN
            IF (purchases>4)
            THEN
                INSERT INTO plaza.membership (ci, points)
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
    AFTER INSERT ON plaza.bill
        FOR EACH ROW 
            EXECUTE PROCEDURE plaza.addMembership()       
    ;"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

# createTriggerAddMembership()


# ADDITIONAL TRIGGERS 
# Trigger that updates the total of a bill whenever he/she adds a product.
def createTriggerUpdateTotal():

    query = """
    CREATE OR REPLACE FUNCTION plaza.updateTotal()
    RETURNS trigger AS $$
    DECLARE 
        date_ date := (
            SELECT MAX(date) FROM plaza.price
            WHERE product_name=NEW.product_name AND id_store=NEW.id_store
        )::date;
        price numeric := (SELECT price FROM plaza.price
                          WHERE product_name = NEW.product_name AND
                          date = date_ AND
                          NEW.id_store = id_store
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
            EXECUTE PROCEDURE plaza.updateTotal()       
    ;"""
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

# def createTriggerUpdateTotal():

#     query = """
#     CREATE OR REPLACE FUNCTION plaza.updateTotal()
#     RETURNS trigger AS $$
#     DECLARE 
#         date_ date := (SELECT datetime FROM plaza.bill WHERE NEW.bill_id=_id)::date;
#         price numeric := (SELECT price FROM plaza.price
#                           WHERE product_name = NEW.product_name AND
#                           date = date_ AND
#                           NEW.id_store = id_store
#                          );
#     BEGIN 
#         UPDATE plaza.bill
#         SET total = (total+(price*NEW.quantity))
#         WHERE NEW.bill_id = _id;
#         RETURN NEW;
#     END;
#     $$ LANGUAGE plpgsql;
#     ;"""

#     cur = conn.cursor()
#     cur.execute(query)
#     cur.close()
#     conn.commit()

#     query = """
#     CREATE TRIGGER updateTotal
#     AFTER INSERT ON plaza.bill_product
#         FOR EACH ROW 
#             EXECUTE PROCEDURE plaza.updateTotal()       
#     ;"""
#     cur = conn.cursor()
#     cur.execute(query)
#     cur.close()
#     conn.commit()

# createTriggerUpdateTotal()




# cur = conn.cursor()
# cur.execute("DROP TRIGGER addMembership ON plaza.bill")
# cur.close()
# conn.commit()
# cur = conn.cursor()
# cur.execute("DROP TRIGGER updateTotal ON plaza.bill_product")
# cur.close()
# conn.commit()
# cur = conn.cursor()
# cur.execute("DROP TRIGGER addPointsWhenPurchase ON plaza.bill_product")
# cur.close()
# conn.commit()
# cur = conn.cursor()
# cur.execute("DROP TRIGGER addPointWhenVisit ON plaza.visit")
# cur.close()
# conn.commit()






