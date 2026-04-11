CREATE OR REPLACE PROCEDURE get_products_by_category(
    category_id INT
)
AS
$$
BEGIN
    SELECT * FROM products WHERE category_id = category_id;
END;
$$ LANGUAGE plpgsql;
