

-- Procedimiento para extraer e insertar datos de ventas
CREATE OR REPLACE PROCEDURE extract_and_load_sales()
AS
$$
BEGIN
    -- Puedes agregar aquí la lógica para extraer datos de la fuente externa
    -- Supongamos que tienes una tabla externa llamada external_sales
    
    -- Inicio de la transacción
    BEGIN
        INSERT INTO orders (customer_id, order_date, status, total_amount)
        SELECT customer_id, order_date, 'Completed'::VARCHAR, total_amount
        FROM external_sales;
        
        -- Confirmación de la transacción
        COMMIT;
    EXCEPTION
        -- Manejo de errores y rollback en caso de falla
        WHEN OTHERS THEN
            RAISE WARNING 'Error durante la carga de datos. %', SQLERRM;
            ROLLBACK;
    END;
END;
$$ LANGUAGE plpgsql;

-- Procedimiento para transformar datos de ventas
CREATE OR REPLACE PROCEDURE transform_sales_data(start_date_param TIMESTAMP DEFAULT '2023-01-01'::TIMESTAMP)
AS
$$
BEGIN
    -- Puedes agregar aquí la lógica para transformar datos según tus requerimientos
    
    -- Inicio de la transacción
    BEGIN
        UPDATE orders
        SET total_amount = total_amount * 0.9  -- Aplicar un descuento del 10%
        WHERE order_date >= start_date_param;  -- Aplicar descuento solo a pedidos después de una fecha específica
        
        -- Confirmación de la transacción
        COMMIT;
    EXCEPTION
        -- Manejo de errores y rollback en caso de falla
        WHEN OTHERS THEN
            RAISE WARNING 'Error durante la transformación de datos. %', SQLERRM;
            ROLLBACK;
    END;
END;
$$ LANGUAGE plpgsql;

-- Procedimiento para ejecutar el ETL completo
CREATE OR REPLACE PROCEDURE run_etl()
AS
$$
BEGIN
    -- Inicio de la transacción global
    BEGIN
        -- Extraer e insertar datos de ventas
        CALL extract_and_load_sales();
        
        -- Transformar datos de ventas
        CALL transform_sales_data();
        
        -- Puedes agregar más pasos según sea necesario
        
        -- Confirmación de la transacción global
        COMMIT;
    EXCEPTION
        -- Manejo de errores y rollback en caso de falla
        WHEN OTHERS THEN
            RAISE WARNING 'Error durante la ejecución del ETL. %', SQLERRM;
            ROLLBACK;
    END;
END;
$$ LANGUAGE plpgsql;
