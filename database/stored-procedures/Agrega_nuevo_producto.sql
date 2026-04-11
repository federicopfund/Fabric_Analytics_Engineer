CREATE OR REPLACE PROCEDURE add_product(
    product_code VARCHAR(20),
    product_name VARCHAR(255),
    category_id INT,
    price DECIMAL(10, 2),
    stock_quantity INT,
    description TEXT
)
AS
$$
BEGIN
    INSERT INTO products (product_code, product_name, category_id, price, stock_quantity, description)
    VALUES (product_code, product_name, category_id, price, stock_quantity, description);
END;
$$ LANGUAGE plpgsql;
