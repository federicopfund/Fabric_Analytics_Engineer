CREATE OR REPLACE VIEW ecommerce.percent_products_on_sale AS
SELECT
    (COUNT(*) FILTER (WHERE on_sale) * 100.0 / COUNT(*)) AS percentage_on_sale
FROM
    ecommerce.products;
