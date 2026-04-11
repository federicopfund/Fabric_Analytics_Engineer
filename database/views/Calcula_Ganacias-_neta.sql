CREATE OR REPLACE VIEW ecommerce.net_profit_per_order AS
SELECT
    o.order_id,
    SUM(od.subtotal - p.cost_price) AS net_profit
FROM
    ecommerce.orders o
INNER JOIN
    ecommerce.order_details od ON o.order_id = od.order_id
INNER JOIN
    ecommerce.products p ON od.product_id = p.product_id
GROUP BY
    o.order_id;