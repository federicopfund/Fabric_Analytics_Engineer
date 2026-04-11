CREATE OR REPLACE PROCEDURE get_customer_sales_total(
    customer_id INT
)
AS
$$
DECLARE
    total DECIMAL(10, 2);
BEGIN
    SELECT COALESCE(SUM(total_amount), 0) INTO total
    FROM orders
    WHERE customer_id = customer_id;

    -- Puedes hacer algo con el total si es necesario
END;
$$ LANGUAGE plpgsql;
