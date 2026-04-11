CREATE OR REPLACE FUNCTION ecommerce.GetOrderDetails(IN p_order_id INT)
RETURNS SETOF RECORD
LANGUAGE plpgsql
AS $$
DECLARE
    order_info ecommerce.orders%ROWTYPE;
    order_details RECORD; -- Puedes ajustar el tipo según tus necesidades
BEGIN
    -- Seleccionar información del pedido
    SELECT * FROM ecommerce.orders WHERE order_id = p_order_id INTO order_info;

    -- Seleccionar detalles del pedido, incluyendo productos asociados
    FOR order_details IN
        SELECT
            od.order_detail_id,
            od.product_id,
            p.product_name,
            od.quantity,
            od.unit_price,
            od.subtotal
        FROM
            ecommerce.order_details od
        INNER JOIN
            ecommerce.products p ON od.product_id = p.product_id
        WHERE
            od.order_id = p_order_id
    LOOP
        -- Devolver los resultados
        RETURN NEXT (
            order_info.order_id,
            order_info.customer_id,
            order_info.order_date,
            order_details.order_detail_id,
            order_details.product_id,
            order_details.product_name,
            order_details.quantity,
            order_details.unit_price,
            order_details.subtotal
        );
    END LOOP;

    -- Finalizar el procedimiento
    RETURN;
END;
$$;
