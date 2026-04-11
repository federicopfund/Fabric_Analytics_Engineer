CREATE OR REPLACE VIEW ecommerce.out_of_stock_products AS
SELECT
    product_id,
    product_name
FROM
    ecommerce.products
WHERE
    stock_quantity = 0;
