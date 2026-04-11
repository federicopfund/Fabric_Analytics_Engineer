CREATE OR REPLACE VIEW ecommerce.monthly_sales AS
SELECT
    EXTRACT(MONTH FROM order_date) AS month,
    SUM(subtotal) AS total_sales
FROM
    ecommerce.order_details
GROUP BY
    EXTRACT(MONTH FROM order_date);
