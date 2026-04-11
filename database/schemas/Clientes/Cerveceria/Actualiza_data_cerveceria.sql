
--Actualiza tabla de Cerveza
MERGE INTO Cerveza AS target
USING (VALUES 
    (1, 'IPA', 6.5, 0.355, 7.99, 'Una IPA refrescante', 60, 10),
    (2, 'Stout', 5.8, 0.573, 8.99, 'tostada', 40, 30),
	 (3, 'Stout', 5.8, 0.973, 8.99, 'Cerveza Rubia', 40, 30),
	  (4, 'Stout', 5.8, 0.273, 8.99, 'Cerveza Stout', 40, 30),
	   (5, 'Stout', 5.8, 0.473, 3.99, 'Cerveza oscura y tostada', 40, 30)
) AS source (CervezaID, Estilo, GraduacionAlcoholica, VolumenLitros, Precio, Descripcion, IBU, SRM)
ON target.CervezaID = source.CervezaID
WHEN MATCHED THEN
    UPDATE SET
        target.Estilo = source.Estilo,
        target.GraduacionAlcoholica = source.GraduacionAlcoholica,
        target.VolumenLitros = source.VolumenLitros,
        target.Precio = source.Precio,
        target.Descripcion = source.Descripcion,
        target.IBU = source.IBU,
        target.SRM = source.SRM
WHEN NOT MATCHED THEN
    INSERT (CervezaID, Estilo, GraduacionAlcoholica, VolumenLitros, Precio, Descripcion, IBU, SRM)
    VALUES (source.CervezaID, source.Estilo, source.GraduacionAlcoholica, source.VolumenLitros, source.Precio, source.Descripcion, source.IBU, source.SRM);

-- Actualiza tabla de Ingredientes
MERGE INTO Ingredientes AS target
USING (VALUES 
    (1, 'Lúpulo', 100, 0.5, 'EE. UU.', 'Lúpulo aromático', '2023-01-15', 'ProveedorA', 5.99, 1),
    (2, 'Malta', 200, 1.0, 'Canadá', 'Malta de cebada', '2023-02-20', 'ProveedorB', 3.99, 2),
	 (3, 'Malta', 200, 1.0, 'Canadá', 'Malta de cebada', '2023-02-20', 'ProveedorB', 3.99, 2)
) AS source (IngredienteID, TipoIngrediente, CantidadStok, CantUnidLitros, Origen, Descripcion, FechaIngreso, Proveedor, PrecioUnitario, CervezaID)
ON target.IngredienteID = source.IngredienteID
WHEN MATCHED THEN
    UPDATE SET
        target.TipoIngrediente = source.TipoIngrediente,
        target.CantidadStok = source.CantidadStok,
        target.CantUnidLitros = source.CantUnidLitros,
        target.Origen = source.Origen,
        target.Descripcion = source.Descripcion,
        target.FechaIngreso = source.FechaIngreso,
        target.Proveedor = source.Proveedor,
        target.PrecioUnitario = source.PrecioUnitario,
        target.CervezaID = source.CervezaID
WHEN NOT MATCHED THEN
    INSERT (IngredienteID, TipoIngrediente, CantidadStok, CantUnidLitros, Origen, Descripcion, FechaIngreso, Proveedor, PrecioUnitario, CervezaID)
    VALUES (source.IngredienteID, source.TipoIngrediente, source.CantidadStok, source.CantUnidLitros, source.Origen, source.Descripcion, source.FechaIngreso, source.Proveedor, source.PrecioUnitario, source.CervezaID);

--Actualiza tabla de Produccion de cerveza
MERGE INTO ProduccionCerveza AS target
USING (VALUES 
    (1, 1, '2023-03-10', 100, 'En proceso'),
    (2, 2, '2023-04-15', 80, 'Completada')
) AS source (ProduccionID, CervezaID, FechaProduccion, CantidadProducida, Estado)
ON target.ProduccionID = source.ProduccionID
WHEN MATCHED THEN
    UPDATE SET
        target.CervezaID = source.CervezaID,
        target.FechaProduccion = source.FechaProduccion,
        target.CantidadProducida = source.CantidadProducida,
        target.Estado = source.Estado
WHEN NOT MATCHED THEN
    INSERT (ProduccionID, CervezaID, FechaProduccion, CantidadProducida, Estado)
    VALUES (source.ProduccionID, source.CervezaID, source.FechaProduccion, source.CantidadProducida, source.Estado);
MERGE INTO PasosProduccion AS target
USING (VALUES 
    (3, 1, 'Maceración', '2023-03-10'),
    (4, 1, 'Fermentación', '2023-03-15')
    -- Agrega más filas según sea necesario
) AS source (PasoID, ProduccionID, Descripcion, FechaPaso)
ON target.PasoID = source.PasoID
WHEN MATCHED THEN
    UPDATE SET
        target.ProduccionID = source.ProduccionID,
        target.Descripcion = source.Descripcion,
        target.FechaPaso = source.FechaPaso
WHEN NOT MATCHED THEN
    INSERT (PasoID, ProduccionID, Descripcion, FechaPaso)
    VALUES (source.PasoID, source.ProduccionID, source.Descripcion, source.FechaPaso);

--Actualiza tabla de inventario cerveza
MERGE INTO InventarioCervezas AS target
USING (VALUES 
    (1, 100),
    (2, 50)
    -- Agrega más filas según sea necesario
) AS source (CervezaID, CantidadStock)
ON target.CervezaID = source.CervezaID
WHEN MATCHED THEN
    UPDATE SET
        target.CantidadStock = source.CantidadStock
WHEN NOT MATCHED THEN
    INSERT (CervezaID, CantidadStock)
    VALUES (source.CervezaID, source.CantidadStock);
-- Actualiza tabla de CLIENTES 

MERGE INTO Clientes AS target
USING (VALUES 
    (1, 'Juan Perez', 'juan@example.com', '123-456-7890', 'Calle Principal 123', 'Ciudad A', 'Pais A', '12345'),
    (2, 'Ana Rodriguez', 'ana@example.com', '987-654-3210', 'Avenida Secundaria 456', 'Ciudad B', 'Pais B', '67890')
    -- Agrega más filas según sea necesario
) AS source (ClienteID, Nombre, Email, Telefono, Direccion, Ciudad, Pais, CodigoPostal)
ON target.ClienteID = source.ClienteID
WHEN MATCHED THEN
    UPDATE SET
        target.Nombre = source.Nombre,
        target.Email = source.Email,
        target.Telefono = source.Telefono,
        target.Direccion = source.Direccion,
        target.Ciudad = source.Ciudad,
        target.Pais = source.Pais,
        target.CodigoPostal = source.CodigoPostal
WHEN NOT MATCHED THEN
    INSERT (ClienteID, Nombre, Email, Telefono, Direccion, Ciudad, Pais, CodigoPostal)
    VALUES (source.ClienteID, source.Nombre, source.Email, source.Telefono, source.Direccion, source.Ciudad, source.Pais, source.CodigoPostal);

	-- Actualiza tabla de Orden
	MERGE INTO Orden AS target
USING (VALUES 
    (1, '2023-03-20', 1, 5, 39.95, 1),
    (2, '2023-04-05', 2, 3, 26.97, 2)
    -- Agrega más filas según sea necesario
) AS source (VentaID, FechaVenta, CervezaID, Cantidad, PrecioTotal, ClienteID)
ON target.VentaID = source.VentaID
WHEN MATCHED THEN
    UPDATE SET
        target.FechaVenta = source.FechaVenta,
        target.CervezaID = source.CervezaID,
        target.Cantidad = source.Cantidad,
        target.PrecioTotal = source.PrecioTotal,
        target.ClienteID = source.ClienteID
WHEN NOT MATCHED THEN
    INSERT (VentaID, FechaVenta, CervezaID, Cantidad, PrecioTotal, ClienteID)
    VALUES (source.VentaID, source.FechaVenta, source.CervezaID, source.Cantidad, source.PrecioTotal, source.ClienteID);


--Actualiza tabla de sucursales
MERGE INTO Subcursales AS target
USING (VALUES 
    (1, 'Subcursal A', 'Avenida Principal 789', 'Ciudad C', 'Pais C', '54321'),
    (2, 'Subcursal B', 'Calle Secundaria 012', 'Ciudad D', 'Pais D', '98765')
    -- Agrega más filas según sea necesario
) AS source (SubcursalID, Nombre, Direccion, Ciudad, Pais, CodigoPostal)
ON target.SubcursalID = source.SubcursalID
WHEN MATCHED THEN
    UPDATE SET
        target.Nombre = source.Nombre,
        target.Direccion = source.Direccion,
        target.Ciudad = source.Ciudad,
        target.Pais = source.Pais,
        target.CodigoPostal = source.CodigoPostal
WHEN NOT MATCHED THEN
    INSERT (SubcursalID, Nombre, Direccion, Ciudad, Pais, CodigoPostal)
    VALUES (source.SubcursalID, source.Nombre, source.Direccion, source.Ciudad, source.Pais, source.CodigoPostal);

--Actualiza tabla de inventario sucursal 
MERGE INTO InventarioSubcursal AS target
USING (VALUES 
    (1, 1, 20),
    (1, 2, 10),
    (2, 1, 15)
    -- Agrega más filas según sea necesario
) AS source (SubcursalID, CervezaID, CantidadStock)
ON target.SubcursalID = source.SubcursalID AND target.CervezaID = source.CervezaID
WHEN MATCHED THEN
    UPDATE SET
        target.CantidadStock = source.CantidadStock
WHEN NOT MATCHED THEN
    INSERT (SubcursalID, CervezaID, CantidadStock)
    VALUES (source.SubcursalID, source.CervezaID, source.CantidadStock);

--Actualiza tabla de inventario sucursal

MERGE INTO EntregasSubcursales AS target
USING (VALUES 
    (1, '2023-03-25', 1, 1, 10, 79.90),
    (2, '2023-04-10', 2, 2, 5, 44.95)
    -- Agrega más filas según sea necesario
) AS source (EntregaID, FechaEntrega, SubcursalDestinoID, CervezaID, CantidadEntregada, PrecioTotalEntrega)
ON target.EntregaID = source.EntregaID
WHEN MATCHED THEN
    UPDATE SET
        target.FechaEntrega = source.FechaEntrega,
        target.SubcursalDestinoID = source.SubcursalDestinoID,
        target.CervezaID = source.CervezaID,
        target.CantidadEntregada = source.CantidadEntregada,
        target.PrecioTotalEntrega = source.PrecioTotalEntrega
WHEN NOT MATCHED THEN
    INSERT (EntregaID, FechaEntrega, SubcursalDestinoID, CervezaID, CantidadEntregada, PrecioTotalEntrega)
    VALUES (source.EntregaID, source.FechaEntrega, source.SubcursalDestinoID, source.CervezaID, source.CantidadEntregada, source.PrecioTotalEntrega);

-- Actualiza tabla de probedores
MERGE INTO Proveedores AS target
USING (VALUES 
    (1, 'Proveedor A', 'Contacto A', '111-222-3333', 'proveedorA@example.com'),
    (2, 'Proveedor B', 'Contacto B', '444-555-6666', 'proveedorB@example.com')
    -- Agrega más filas según sea necesario
) AS source (ProveedorID, Nombre, Contacto, Telefono, Email)
ON target.ProveedorID = source.ProveedorID
WHEN MATCHED THEN
    UPDATE SET
        target.Nombre = source.Nombre,
        target.Contacto = source.Contacto,
        target.Telefono = source.Telefono,
        target.Email = source.Email
WHEN NOT MATCHED THEN
    INSERT (ProveedorID, Nombre, Contacto, Telefono, Email)
    VALUES (source.ProveedorID, source.Nombre, source.Contacto, source.Telefono, source.Email);


--Actualiza tabla de Proveedores sucursal
MERGE INTO ProveedoresSubcursales AS target
USING (VALUES 
    (1, 1),
    (2, 2)
    -- Agrega más filas según sea necesario
) AS source (SubcursalID, ProveedorID)
ON target.SubcursalID = source.SubcursalID AND target.ProveedorID = source.ProveedorID
WHEN NOT MATCHED THEN
    INSERT (SubcursalID, ProveedorID)
    VALUES (source.SubcursalID, source.ProveedorID);
