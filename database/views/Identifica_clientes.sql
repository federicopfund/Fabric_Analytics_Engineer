CREATE OR REPLACE VIEW ecommerce.top_customers AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS total_orders
FROM
    ecommerce.customers c
INNER JOIN
    ecommerce.orders o ON c.customer_id = o.customer_id
GROUP BY
    c.customer_id, c.first_name, c.last_name
ORDER BY
    total_orders DESC;
