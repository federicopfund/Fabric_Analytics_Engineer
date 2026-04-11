CREATE OR REPLACE VIEW ecommerce.recent_customers AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    MAX(o.order_date) AS last_purchase_date
FROM
    ecommerce.customers c
INNER JOIN
    ecommerce.orders o ON c.customer_id = o.customer_id
GROUP BY
    c.customer_id, c.first_name, c.last_name
ORDER BY
    last_purchase_date DESC;
