-- Crear una vista que muestra el promedio del precio de los productos
CREATE OR REPLACE VIEW ecommerce.average_product_price AS
SELECT
    category_id,
    AVG(price) AS avg_price
FROM
    ecommerce.products
GROUP BY
    category_id;
