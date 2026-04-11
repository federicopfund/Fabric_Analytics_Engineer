CREATE OR REPLACE PROCEDURE add_customer(
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(255),
    password VARCHAR(255),
    address VARCHAR(255)
)
AS
$$
BEGIN
    INSERT INTO customers (first_name, last_name, email, password, address)
    VALUES (first_name, last_name, email, password, address);
END;
$$ LANGUAGE plpgsql;
