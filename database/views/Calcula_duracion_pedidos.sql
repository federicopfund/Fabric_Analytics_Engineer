CREATE OR REPLACE VIEW ecommerce.avg_time_between_orders AS
SELECT
    customer_id,
    AVG(EXTRACT(EPOCH FROM (order_date - LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)))) AS avg_time
FROM
    ecommerce.orders
GROUP BY
    customer_id;
