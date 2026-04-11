-- Crear una vista que muestra los productos más vendidos
CREATE OR REPLACE VIEW ecommerce.most_sold_products AS
SELECT
    p.product_id,
    p.product_name,
    SUM(od.quantity) AS total_sold
FROM
    ecommerce.products p
INNER JOIN
    ecommerce.order_details od ON p.product_id = od.product_id
GROUP BY
    p.product_id, p.product_name
ORDER BY
    total_sold DESC;
