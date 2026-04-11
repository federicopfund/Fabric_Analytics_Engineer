CREATE OR REPLACE PROCEDURE get_inventory()
AS
$$
BEGIN
    SELECT * FROM products;
END;
$$ LANGUAGE plpgsql;
