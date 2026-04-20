Clear-Host
Write-Host "Starting script at $(Get-Date)"

# Generate unique random suffix
[string]$suffix = -join ((48..57) + (97..122) | Get-Random -Count 7 | % {[char]$_})
Write-Host "Your randomly-generated suffix for Azure resources is $suffix"

$username = ""
$password = ""


Write-Host "Se definen las variables de los servicios.";

$accountName = "storageblobs$suffix"
$containerName = "containerbacpac$suffix"
$containerName2 = "landing$suffix"
$location = "East US"
$rolqueue = "Storage Queue Data Contributor"
$rolblobs = "Storage Blob Data Contributor"

# variables para el servidor SQL
$serverName = "server--$suffix"
$adminSqlLogin = "admin25"
$adminSqlPassword = "Siglo2510"

# nombre base de dato

$databaseName = "dbRetail"

$namedbbacpac = "dbRetail.bacpac"

# Configuracion DataLake V2
$DataLakeName = "datalake$suffix"
$filesystemName = "sink"
$dirname = "sink"

# variebles databricks
$databricks ="databricks-$suffix"

# variables para data factory

$dataFactoryName = "adfactory-$suffix"

## Link service
$azureSqlDatabaseLinkedService = "azureSqlDatabaseLinkedService"


Write-Host "Connect a Azure a la cuenta de $username..";
#-------------LOGIN--------------


$SecurePassword = ConvertTo-SecureString $password -AsPlainText -Force
$credentials = New-Object System.Management.Automation.PSCredential($username, $SecurePassword)


try {
    Login-AzAccount -Credential $credentials -ErrorAction Stop
    Write-Host "Inicio de sesión correcto."
    $loginSuccessful = $true
} catch {
     Handle-Error $_.Exception.Message
    exit 1
}

function Handle-Error {
    param (
        [string]$errorMessage
    )
    Write-Host "Error: $errorMessage"
    # Add additional actions to handle the error if needed
}

# Function to validate operation results
function Validate-Operation {
    param (
        [string]$operationName,
        [object]$result
    )
    if ($result -eq $null) {
        Handle-Error "$operationName failed."
        exit 1
    }
    else {
        Write-Host "$operationName succeeded."
    }
}


# Other variable declarations.
if ($loginSuccessful) {
    # Verificar si el grupo de recursos ya existe
    $existingResourceGroup = Get-AzResourceGroup -Name $rgName -ErrorAction SilentlyContinue
    if ($existingResourceGroup -eq $null) {
        # Crear el grupo de recursos si no existe
        $resourceGroupLocation = "East US"  # Cambia según tu ubicación preferida
        $newResourceGroup = New-AzResourceGroup -Name $rgName -Location $resourceGroupLocation

        # Validar la operación
        Validate-Operation "Creating Resource Group" $newResourceGroup
    }
    $tenandID = (Get-AzContext).Tenant.TenantId
    Write-Host "Tenant ID obtenido: $tenandID"
    # -------------CREAR STORAGE ACCOUNT, CONTAINER Y SUBIR BACPAC------------

    Write-Host "Storage Account...";
    $StorageAccount = Get-AzStorageAccount -ResourceGroupName $rgName -Name $accountName -ErrorAction SilentlyContinue

    if ($StorageAccount -eq $null) {
        # El Storage Account no existe, así que lo creamos
        $StorageAccount = New-AzStorageAccount -ResourceGroupName $rgName `
            -Name $accountName `
            -SkuName Standard_RAGRS `
            -Location $location `
            -Kind StorageV2 

        Write-Host "Storage Account creado."
    } else {
        # El Storage Account ya existe, puedes manejar esto según tus necesidades
        Write-Host "El Storage Account ya existe. Puedes manejar esto según tus necesidades."
    }

    $StorageAccount

    Write-Host "Roles de Storage Container...";

    # Verifica si el contenedor ya existe
    $containerExists = Get-AzStorageContainer -Name $containerName -Context $StorageAccount.Context -ErrorAction SilentlyContinue

    if ($null -eq $containerExists) {
        Write-Host "Creando el contenedor $containerName..."
        New-AzStorageContainer -Name $containerName -Context $StorageAccount.Context -Permission blob
    } else {
        Write-Host "El contenedor $containerName ya existe. No es necesario crearlo nuevamente."
    }

    # Repite el proceso para el segundo contenedor
    $container2Exists = Get-AzStorageContainer -Name $containerName2 -Context $StorageAccount.Context -ErrorAction SilentlyContinue

    if ($null -eq $container2Exists) {
        Write-Host "Creando el contenedor $containerName2..."
        New-AzStorageContainer -Name $containerName2 -Context $StorageAccount.Context -Permission Container
    } else {
        Write-Host "El contenedor $containerName2 ya existe. No es necesario crearlo nuevamente."
    }

    # Creo un storage account para el datalake con acceso jerárquico
    Write-host "Creo el Data lake..."

    # Verifica si la cuenta de almacenamiento del Data Lake ya existe
    $dataLakeExists = Get-AzStorageAccount -ResourceGroupName $rgName -Name $DataLakeName -ErrorAction SilentlyContinue

    if ($null -eq $dataLakeExists) {
        # Crea la cuenta de almacenamiento del Data Lake
        $azureDataLake = New-AzStorageAccount -ResourceGroupName $rgName `
                                              -Name $DataLakeName `
                                              -Location $location `
                                              -SkuName Standard_RAGRS `
                                              -Kind StorageV2 `
                                              -EnableHierarchicalNamespace $True

        Write-Host "Creada la cuenta de almacenamiento del Data Lake: $($azureDataLake.StorageAccountName)"
    } else {
        Write-Host "La cuenta de almacenamiento del Data Lake ya existe. No es necesario crearla nuevamente."
    }
    
    # Agrego Permisos al contexto de $azureDataLake
    Write-host "Agrego Permisos al contex de $azureDataLake..."

    $filesystemExists = Get-AzStorageContainer -Context $azureDataLake.Context -Name $filesystemName -ErrorAction SilentlyContinue

    if ($null -eq $filesystemExists) {
        # Crear el contenedor si no existe
        New-AzStorageContainer -Name $filesystemName -Context $azureDataLake.Context -Permission Blob
        Write-Host "Creado el contenedor '$filesystemName' en la cuenta de almacenamiento del Data Lake."
    } else {
        Write-Host "El contenedor '$filesystemName' ya existe en la cuenta de almacenamiento del Data Lake. No es necesario crearlo nuevamente."
    }

    # Extraigo claves de las dos cuentas de almacenamiento
    $storageAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $rgName -Name $accountName).Value[0]
    $dataLakeAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $rgName -Name $DataLakeName).Value[0]
    # Subo el bacpac
    Write-Host "Deploy bacpac..."

    $blobExists = Get-AzStorageBlob -Context $storageAccount.Context -Container $containerName -Blob $namedbbacpac -ErrorAction SilentlyContinue

    if ($null -eq $blobExists) {
        # Subir el bacpac solo si no existe
        $StorageBlobContent = @{
            File             = "$(Get-Location)\$namedbbacpac"
            Container        = $containerName
            Blob             = $namedbbacpac
            Context          = $storageAccount.Context
            StandardBlobTier = 'Hot'
        }

        $Estatus_deploy = Set-AzStorageBlobContent @StorageBlobContent
        Write-Host "Usted subió el bacpac el día:" $Estatus_deploy.LastModified.Date
    } else {
        Write-Host "El bacpac '$namedbbacpac' ya existe en el contenedor '$containerName'. No es necesario subirlo nuevamente."
    }
    $existingServer = Get-AzSqlServer -ResourceGroupName $rgName -ServerName $serverName -ErrorAction SilentlyContinue

    if ($null -eq $existingServer) {
        # El servidor SQL no existe, crea uno nuevo
        Write-Host "Deploy Server SQL..."

        $server = New-AzSqlServer -ResourceGroupName $rgName `
            -ServerName $serverName `
            -Location $location `
            -SqlAdministratorCredentials $(New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $adminSqlLogin, $(ConvertTo-SecureString -String $adminSqlPassword -AsPlainText -Force))

        $server
    } else {
        Write-Host "El servidor SQL '$serverName' ya existe en el grupo de recursos '$rgName'. No es necesario crear uno nuevo."
    }
    $existingFirewallRule = Get-AzSqlServerFirewallRule -ResourceGroupName $rgName -ServerName $serverName -FirewallRuleName "AllowMyPublicIP" -ErrorAction SilentlyContinue

    if ($null -eq $existingFirewallRule) {
        # La regla de firewall no existe, crea una nueva
        Write-Host "Rule de Firewail..."

        $myPublicIp = (Invoke-WebRequest -Uri "http://ipinfo.io/ip" -UseBasicParsing).content

        $IP = New-AzSqlServerFirewallRule -ResourceGroupName $rgName -ServerName $serverName -AllowAllAzureIPs
        New-AzSqlServerFirewallRule -ResourceGroupName $rgName -ServerName $serverName -FirewallRuleName "AllowMyPublicIP" -StartIpAddress $IP.StartIpAddress -EndIpAddress $myPublicIP

        Write-Host "Se ha creado la regla de firewall 'AllowMyPublicIP' para permitir la dirección IP pública $myPublicIP."
    } else {
        Write-Host "La regla de firewall 'AllowMyPublicIP' ya existe en el servidor SQL '$serverName'. No es necesario crear una nueva."
    }
    $existingDatabase = Get-AzSqlDatabase -ResourceGroupName $rgName -ServerName $server.ServerName -DatabaseName $databaseName -ErrorAction SilentlyContinue

    if ($null -eq $existingDatabase) {
        # La base de datos no existe, crea una nueva
        Write-Host "Creando base de dato..."
    
        # Convertir la contraseña en SecureString
        $adminSqlPasswordSecure = ConvertTo-SecureString $adminSqlPassword -AsPlainText -Force

        $database = New-AzSqlDatabase -ComputeGeneration Gen5 `
                                        -DatabaseName $databaseName `
                                        -Edition GeneralPurpose `
                                        -ResourceGroupName $rgName `
                                        -ServerName $server.ServerName `
                                        -VCore 2

        Write-Host "La base de datos '$databaseName' ha sido creada en el servidor SQL '$($server.ServerName)'."
    } else {
        Write-Host "La base de datos '$databaseName' ya existe en el servidor SQL '$($server.ServerName)'. No es necesario crear una nueva."
    }
        # Importamos Bacpac
    Write-Host "Importando el bacpac a la base de datos. Esto puede tomar unos minutos..."

    try {
        $Statusimport = New-AzSqlDatabaseImport `
                        -ResourceGroupName $rgName `
                        -ServerName $server.ServerName `
                        -DatabaseName $database.DatabaseName `
                        -StorageKeyType "StorageAccessKey" `
                        -StorageKey $storageAccountKey `
                        -StorageUri "https://$accountName.blob.core.windows.net/$containerName/$namedbbacpac" `
                        -AdministratorLogin $adminSqlLogin `
                        -AdministratorLoginPassword $adminSqlPasswordSecure `
                        -Edition Standard `
                        -ServiceObjectiveName S0 `
                        -DatabaseMaxSizeBytes 100GB -ErrorAction Stop

        Write-Host "Importación completada. Estado: $($Statusimport.Status)"
    }
    catch {
        Write-Host "Error al importar el bacpac: $_"
    }
        # Crear Azure Databricks workspace
    Write-Host "Creando el espacio de trabajo Azure Databricks en el grupo de recursos $rgName..."

    try {
        # Desplegar plantilla de Azure Resource Manager (ARM)
        #New-AzResourceGroupDeployment -ResourceGroupName $rgName -TemplateFile ./databricks/main.bicep -workspaceName $databricks -ErrorAction Stop

        # Crear el espacio de trabajo Azure Databricks
        $databrick = New-AzDatabricksWorkspace -Name $databricks -ResourceGroupName $rgName -Location $location -Sku standard -ErrorAction Stop

        Write-Host "Espacio de trabajo Azure Databricks creado con éxito. Nombre: $($databrick.WorkspaceName)"
    }
    catch {
        Write-Host "Error al crear el espacio de trabajo Azure Databricks: $_"
    }
    # Crear Azure Data Factory V2
    Write-Host "Creando Azure Data Factory V2..."

    try {
        # Crear Data Factory
        $adf = Set-AzDataFactoryV2 -ResourceGroupName $rgName -Name $dataFactoryName -Location $location -ErrorAction Stop

        Write-Host "Azure Data Factory V2 creado con éxito. Nombre: $($adf.DataFactoryName)"
        Write-Host "Estado del Data Factory: $($adf.ProvisioningState)"
    }
    catch {
        Write-Host "Error al crear Azure Data Factory V2: $_"
    }

    # Crear y configurar los Linked Services
    Write-Host "Configurando Linked Services..."

    try {
        # Definir el Linked Service para Azure SQL Database
        $azureSQLDatabaseLinkedServiceDefinition = @"
        {
            "name": "$azureSqlDatabaseLinkedService",
            "type": "Microsoft.DataFactory/factories/linkedservices",
            "properties": {
                "annotations": [],
                "type": "AzureSqlDatabase",
                "typeProperties": {
                    "connectionString": "integrated security=False;encrypt=True;connection timeout=30;data source=$serverName.database.windows.net;initial catalog=$databaseName;user id=$adminSqlLogin;password = $adminSqlPassword"
                }
            }
        }
    "@

    # Guardar definición en un archivo
    $azureSQLDatabaseLinkedServiceDefinition | Out-File ".\$azureSqlDatabaseLinkedService.json"

    # Configurar Linked Service de Azure SQL Database
    Set-AzDataFactoryV2LinkedService -DataFactoryName $dataFactoryName -ResourceGroupName $rgName -Name "AzureSqlDatabaseLinkedService" -DefinitionFile ".\$azureSqlDatabaseLinkedService.json"

    # Definir el Linked Service para Azure Blob Storage
    $azureStorageLinkedServiceDefinition = @"
        {
            "name": "$azureStorageLinkedService",
            "properties": {
                "annotations": [],
                "type": "AzureBlobStorage",
                "typeProperties": {
                    "connectionString": "DefaultEndpointsProtocol=https;AccountName=$accountName;AccountKey=$storageKey;EndpointSuffix=core.windows.net"
                }
            }
        }
    "@

    # Guardar definición en un archivo
    $azureStorageLinkedServiceDefinition | Out-File ".\$azureStorageLinkedService.json"

    # Configurar Linked Service de Azure Blob Storage
    Set-AzDataFactoryV2LinkedService -DataFactoryName $dataFactoryName -ResourceGroupName $rgName -Name "AzureStorageLinkedService" -DefinitionFile ".\$azureStorageLinkedService.json"

    # Definir el Linked Service para Azure Databricks
    $azurDatabricksLinkedServiceDefinition = @"
    {
        "name": "$azureDatabricksLinkedService",
        "properties": {
            "annotations": [],
            "type": "AzureDatabricks",
            "typeProperties": {
                "domain": "$domainDatabricks",
                "authentication": "MSI",
                "workspaceResourceId": "/subscriptions/$selectedSub/resourceGroups/$rgName/providers/Microsoft.Databricks/workspaces/$databricks",
                "newClusterNodeType": "Standard_DS3_v2",
                "newClusterNumOfWorker": "1",
                "newClusterVersion": "10.4.x-scala2.12"
            }
        },
        "type": "Microsoft.DataFactory/factories/linkedservices"
    }
"@

    # Guardar definición en un archivo
    $azurDatabricksLinkedServiceDefinition | Out-File ".\$azurDatabricksLinkedService.json"

    # Configurar Linked Service de Azure Databricks
    Set-AzDataFactoryV2LinkedService -DataFactoryName $dataFactoryName -ResourceGroupName $rgName -Name "AzureDatabricksLinkedService" -DefinitionFile ".\$azurDatabricksLinkedService.json"

    Write-Host "Linked Services configurados con éxito."
    }
    catch {
        Write-Host "Error al configurar Linked Services: $_"
    }
    Set-AzDataFactoryV2LinkedService -DataFactoryName $dataFactoryName -ResourceGroupName $rgName -Name "AzureDatabricksLinkedService" -DefinitionFile ".\$azurDatabricksLinkedService.json"

    # guardo las claves en un txt
    Write-host "Guardando claves en un archivo TXT"

    "$accountName"|Add-Content keys.txt
    "$storageAccountKey"|Add-Content keys.txt
    "$DataLakeName"|Add-Content keys.txt
    "$dataLakeAccountKey"|Add-Content keys.txt

    # subo el .txt al blob
    Write-host "Subiendo claves al $containerName"

    $Keys = @{
      File             = ".\keys.txt"
      Container        = $containerName
      Blob             = "keys.txt"
      Context          = $StorageAccount.Context
      StandardBlobTier = 'Hot'
    }

    Set-AzStorageBlobContent @Keys


    write-host "Script completed at $(Get-Date)"
    } else {
    Write-Host "No se pudo iniciar sesión en Azure. Verifica las credenciales y vuelve a intentarlo."
    # Puedes manejar esto según tus necesidades, como salir del script o solicitar al usuario que proporcione credenciales válidas.
    exit
}


