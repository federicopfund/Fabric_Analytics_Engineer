CREATE OR REPLACE VIEW ecommerce.stock_quantity AS
SELECT
    product_id,
    product_name,
    stock_quantity
FROM
    ecommerce.products;
