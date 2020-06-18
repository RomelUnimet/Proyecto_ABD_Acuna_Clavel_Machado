query = """
    SELECT name, i.qty_available FROM plaza.product AS p
    INNER JOIN plaza.shelf AS s ON s.product_name = p.name
    INNER JOIN plaza.in_stock AS i ON s._id = i.shelf_id
    WHERE p.id_store = 1 AND
          s.id_store = 1 AND
          i.id_store = 1 AND
          i.datetime = (
              SELECT MAX(plaza.in_stock.datetime) FROM plaza.in_stock 
              INNER JOIN plaza.shelf AS sh ON plaza.in_stock.shelf_id = sh._id
              WHERE sh.product_name = p.name AND
                    sh.id_store = 1 AND
                    p.id_store=1
          )
;"""