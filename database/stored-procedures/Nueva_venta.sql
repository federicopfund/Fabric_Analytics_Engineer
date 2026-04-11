CREATE OR REPLACE PROCEDURE make_sale(
    customer_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10, 2)
)
AS
$$
DECLARE
    order_id INT; -- Declarar la variable order_id
    subtotal DECIMAL(10, 2);
BEGIN
    -- Insertar un nuevo pedido
    INSERT INTO orders (customer_id, order_date, status, total_amount)
    VALUES (customer_id, CURRENT_TIMESTAMP, 'Completed', 0)
    RETURNING order_id INTO order_id;

    -- Insertar detalles del pedido
    INSERT INTO order_details (order_id, product_id, quantity, unit_price, subtotal)
    VALUES (order_id, product_id, quantity, unit_price, unit_price * quantity);

    -- Actualizar el monto total del pedido
    UPDATE orders
    SET total_amount = (SELECT SUM(subtotal) FROM order_details WHERE order_id = order_id)
    WHERE order_id = order_id;

    -- Actualizar la cantidad de stock del producto
    UPDATE products
    SET stock_quantity = stock_quantity - quantity
    WHERE product_id = product_id;
END;
$$ LANGUAGE plpgsql;
