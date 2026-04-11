
--CREATE DATABASE cerveceria;


-- Tabla para almacenar información de las cervezas
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Cerveza')
BEGIN
    -- Crear la tabla Cervezas
    CREATE TABLE Cerveza (
        CervezaID INT PRIMARY KEY,
        Estilo VARCHAR(50) NOT NULL,
        GraduacionAlcoholica DECIMAL(3, 1),
        VolumenLitros DECIMAL(4, 2),
        Precio DECIMAL(5, 2) NOT NULL,
		Descripcion TEXT,
        IBU INT,
        SRM INT
    );
	
END

-- Tabla para almacenar información de los ingredientes de las cervezas
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Ingredientes')
BEGIN
    -- Crear la tabla Ingredientes
    CREATE TABLE Ingredientes (
        IngredienteID INT PRIMARY KEY,
        TipoIngrediente VARCHAR(50) NOT NULL,
		CantidadStok INT,
		CantUnidLitros DECIMAL(8, 4),
        Origen VARCHAR(50),
        Descripcion TEXT,
        FechaIngreso DATE,
        Proveedor VARCHAR(50),
        PrecioUnitario DECIMAL(8, 2),
        CervezaID INT,
        FOREIGN KEY (CervezaID) REFERENCES Cerveza(CervezaID)
    );
	
END


-- Tablas para la produccion de cerveza.
-- Tabla para almacenar información sobre la producción de cerveza
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ProduccionCerveza')
BEGIN
    -- Crear la tabla ProduccionCerveza
    CREATE TABLE ProduccionCerveza (
        ProduccionID INT PRIMARY KEY,
        CervezaID INT,
        FechaProduccion DATE NOT NULL,
        CantidadProducida DECIMAL(8, 2) NOT NULL,
        Estado VARCHAR(50) NOT NULL,
        FOREIGN KEY (CervezaID) REFERENCES Cerveza(CervezaID)
    );
END

-- Tabla para almacenar información sobre los pasos de producción de cerveza
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PasosProduccion')
BEGIN
    -- Crear la tabla PasosProduccion
    CREATE TABLE PasosProduccion (
        PasoID INT PRIMARY KEY,
        ProduccionID INT,
        Descripcion VARCHAR(100) NOT NULL,
        FechaPaso DATE NOT NULL,
        FOREIGN KEY (ProduccionID) REFERENCES ProduccionCerveza(ProduccionID)
    );

END


-- Tablas para el manejo de Stock
-- Tabla para almacenar información sobre el inventario de cervezas
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'InventarioCervezas')
BEGIN
    -- Crear la tabla InventarioCervezas
    CREATE TABLE InventarioCervezas (
        CervezaID INT PRIMARY KEY,
        CantidadStock DECIMAL(8, 2) NOT NULL,
        FOREIGN KEY (CervezaID) REFERENCES Cerveza(CervezaID)
    );
END
-- Tabla para almacenar información sobre los clientes
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Clientes')
BEGIN
    -- Crear la tabla Clientes
    CREATE TABLE Clientes (
        ClienteID INT PRIMARY KEY,
        Nombre VARCHAR(50) NOT NULL,
        Email VARCHAR(50),
        Telefono VARCHAR(15),
        Direccion VARCHAR(100),
        Ciudad VARCHAR(50),
        Pais VARCHAR(50),
        CodigoPostal VARCHAR(10)
    );
END


-- Tabla para almacenar información sobre las ventas de cervezas
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Orden')
BEGIN
    -- Crear la tabla Orden
    CREATE TABLE Orden (
        VentaID INT IDENTITY(1,1) PRIMARY KEY, -- Definir VentaID como PK y autoincremental
        FechaVenta DATE NOT NULL,
        CervezaID INT,
        Cantidad DECIMAL(8, 2) NOT NULL,
        PrecioTotal DECIMAL(8, 2) NOT NULL,
        ClienteID INT,
        FOREIGN KEY (CervezaID) REFERENCES Cerveza(CervezaID),
        FOREIGN KEY (ClienteID) REFERENCES Clientes(ClienteID)
    );
END


-- Tablas para la distribucion

-- Tabla para almacenar información de las subcursales
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Subcursales')
BEGIN
    -- Crear la tabla Subcursales
    CREATE TABLE Subcursales (
        SubcursalID INT PRIMARY KEY,
        Nombre VARCHAR(50) NOT NULL,
        Direccion VARCHAR(100),
        Ciudad VARCHAR(50),
        Pais VARCHAR(50),
        CodigoPostal VARCHAR(10)
    );
END

-- Tabla para almacenar información de los productos en cada subcursal
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'InventarioSubcursal')
BEGIN
    -- Crear la tabla InventarioSubcursal
    CREATE TABLE InventarioSubcursal (
        SubcursalID INT,
        CervezaID INT,
        CantidadStock DECIMAL(8, 2) NOT NULL,
        PRIMARY KEY (SubcursalID, CervezaID),
        FOREIGN KEY (SubcursalID) REFERENCES Subcursales(SubcursalID),
        FOREIGN KEY (CervezaID) REFERENCES Cerveza(CervezaID)
    );
END

-- Tabla para almacenar información sobre las entregas a subcursales
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'EntregasSubcursales')
BEGIN
	CREATE TABLE EntregasSubcursales (
		EntregaID INT PRIMARY KEY,
		FechaEntrega DATE NOT NULL,
		SubcursalDestinoID INT,
		CervezaID INT,
		CantidadEntregada DECIMAL(8, 2) NOT NULL,
		PrecioTotalEntrega DECIMAL(8, 2) NOT NULL,
		FOREIGN KEY (SubcursalDestinoID) REFERENCES Subcursales(SubcursalID),
		FOREIGN KEY (CervezaID) REFERENCES Cerveza(CervezaID)
	);
END

-- Tabla para almacenar información sobre los proveedores
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Proveedores')
BEGIN
	CREATE TABLE Proveedores (
		ProveedorID INT PRIMARY KEY,
		Nombre VARCHAR(50) NOT NULL,
		Contacto VARCHAR(50),
		Telefono VARCHAR(15),
		Email VARCHAR(50)
	);
END
-- Tabla para almacenar información sobre los proveedores de las subcursales
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ProveedoresSubcursales')
BEGIN
	CREATE TABLE ProveedoresSubcursales (
		SubcursalID INT,
		ProveedorID INT,
		PRIMARY KEY (SubcursalID, ProveedorID),
		FOREIGN KEY (SubcursalID) REFERENCES Subcursales(SubcursalID),
		FOREIGN KEY (ProveedorID) REFERENCES Proveedores(ProveedorID)
	);
END

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TransaccionesInventario')
BEGIN
    -- Create the table TransaccionesInventario
    CREATE TABLE TransaccionesInventario (
        TransaccionID INT PRIMARY KEY,
        CervezaID INT,
        FechaTransaccion DATE NOT NULL,
        CantidadAntes DECIMAL(8, 2) NOT NULL,
        CantidadDespues DECIMAL(8, 2) NOT NULL,
        TipoTransaccion VARCHAR(20) NOT NULL,
        FOREIGN KEY (CervezaID) REFERENCES Cerveza(CervezaID)
    );
END;

-- Añadir tabla de historial de precios
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'HistorialPrecios')
BEGIN
    -- Create the table HistorialPrecios
    CREATE TABLE HistorialPrecios (
        HistorialID INT PRIMARY KEY,
        CervezaID INT,
        FechaCambio DATE NOT NULL,
        PrecioAntes DECIMAL(5, 2) NOT NULL,
        PrecioDespues DECIMAL(5, 2) NOT NULL,
        FOREIGN KEY (CervezaID) REFERENCES Cerveza(CervezaID)
    );
END;

-- Añadir índices si no existen

-- Índice en la tabla Cerveza
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Cerveza_Estilo' AND object_id = OBJECT_ID('Cerveza'))
BEGIN
    CREATE INDEX IX_Cerveza_Estilo ON Cerveza(Estilo);
END

-- Índice en la tabla Ingredientes
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Ingredientes_CervezaID' AND object_id = OBJECT_ID('Ingredientes'))
BEGIN
    CREATE INDEX IX_Ingredientes_CervezaID ON Ingredientes(CervezaID);
END

-- Índice en la tabla ProduccionCerveza
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ProduccionCerveza_CervezaID' AND object_id = OBJECT_ID('ProduccionCerveza'))
BEGIN
    CREATE INDEX IX_ProduccionCerveza_CervezaID ON ProduccionCerveza(CervezaID);
END

-- Índice en la tabla PasosProduccion
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_PasosProduccion_ProduccionID' AND object_id = OBJECT_ID('PasosProduccion'))
BEGIN
    CREATE INDEX IX_PasosProduccion_ProduccionID ON PasosProduccion(ProduccionID);
END

-- Índice en la tabla InventarioCervezas
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_InventarioCervezas_CervezaID' AND object_id = OBJECT_ID('InventarioCervezas'))
BEGIN
    CREATE INDEX IX_InventarioCervezas_CervezaID ON InventarioCervezas(CervezaID);
END

-- Índice en la tabla Orden
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Orden_ClienteID' AND object_id = OBJECT_ID('Orden'))
BEGIN
    CREATE INDEX IX_Orden_ClienteID ON Orden(ClienteID);
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Orden_CervezaID' AND object_id = OBJECT_ID('Orden'))
BEGIN
    CREATE INDEX IX_Orden_CervezaID ON Orden(CervezaID);
END

-- Índice en la tabla EntregasSubcursales
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_EntregasSubcursales_CervezaID' AND object_id = OBJECT_ID('EntregasSubcursales'))
BEGIN
    CREATE INDEX IX_EntregasSubcursales_CervezaID ON EntregasSubcursales(CervezaID);
END

-- Índice en la tabla ProveedoresSubcursales
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_ProveedoresSubcursales_ProveedorID' AND object_id = OBJECT_ID('ProveedoresSubcursales'))
BEGIN
    CREATE INDEX IX_ProveedoresSubcursales_ProveedorID ON ProveedoresSubcursales(ProveedorID);
END


-- Añadir claves únicas compuestas si no existen

-- Clave única compuesta en la tabla Ingredientes
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'UQ_Ingredientes' AND object_id = OBJECT_ID('Ingredientes'))
BEGIN
    ALTER TABLE Ingredientes
    ADD CONSTRAINT UQ_Ingredientes UNIQUE (IngredienteID, CervezaID);
END

-- Clave única compuesta en la tabla InventarioSubcursal
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'UQ_InventarioSubcursal' AND object_id = OBJECT_ID('InventarioSubcursal'))
BEGIN
    ALTER TABLE InventarioSubcursal
    ADD CONSTRAINT UQ_InventarioSubcursal UNIQUE (SubcursalID, CervezaID);
END





