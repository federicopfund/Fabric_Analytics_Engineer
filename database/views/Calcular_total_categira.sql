CREATE OR REPLACE VIEW ecommerce.category_revenue AS
SELECT
    c.category_id,
    c.category_name,
    SUM(od.subtotal) AS total_revenue
FROM
    ecommerce.categories c
INNER JOIN
    ecommerce.products p ON c.category_id = p.category_id
INNER JOIN
    ecommerce.order_details od ON p.product_id = od.product_id
GROUP BY
    c.category_id, c.category_name;
