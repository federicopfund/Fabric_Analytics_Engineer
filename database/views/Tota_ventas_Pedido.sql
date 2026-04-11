-- Crear una vista que muestra el total de ventas por pedido
CREATE OR REPLACE VIEW ecommerce.order_sales_total AS
SELECT
    o.order_id,
    SUM(od.subtotal) AS total_sales
FROM
    ecommerce.orders o
INNER JOIN
    ecommerce.order_details od ON o.order_id = od.order_id
GROUP BY
    o.order_id;
