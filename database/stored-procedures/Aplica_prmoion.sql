CREATE OR REPLACE PROCEDURE apply_promotion_to_order(
    order_id INT,
    promotion_id INT
)
AS
$$
BEGIN
    INSERT INTO order_promotions (order_id, promotion_id)
    VALUES (order_id, promotion_id);
END;
$$ LANGUAGE plpgsql;
