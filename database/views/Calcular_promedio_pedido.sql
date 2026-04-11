CREATE OR REPLACE VIEW ecommerce.avg_products_per_order AS
SELECT
    AVG(product_count) AS avg_products_per_order
FROM
    (SELECT
        order_id,
        COUNT(product_id) AS product_count
    FROM
        ecommerce.order_details
    GROUP BY
        order_id) AS product_counts;
