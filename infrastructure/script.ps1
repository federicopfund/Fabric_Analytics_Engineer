Clear-Host
write-host "Starting script at $(Get-Date)"
# Generate unique random suffix
[string]$suffix =  -join ((48..57) + (97..122) | Get-Random -Count 7 | % {[char]$_})
Write-Host "Your randomly-generated suffix for Azure resources is $suffix"


$username = ""
$password = ""

Write-Host "Connect a Azure a la cuenta de $username..";
#-------------LOGIN--------------


$SecurePassword = ConvertTo-SecureString $password -AsPlainText -Force
$credentials = New-Object System.Management.Automation.PSCredential($username, $SecurePassword)
Login-AzAccount -Credential $credentials

Write-Host "login correcto.";

$rgName = "Rgfedericopfund"
Write-Host "los Servicios se crearan en el grupo de recurso $rgName.";

Write-Host "Los servicios se crearán en el grupo de recurso $rgName."

# Verificar si el grupo de recursos ya existe
$existingResourceGroup = Get-AzResourceGroup -Name $rgName -ErrorAction SilentlyContinue

if ($existingResourceGroup -eq $null) {
    # Crear el grupo de recursos si no existe
    $resourceGroupLocation = "East US"  # Cambia según tu ubicación preferida
    $rgName = New-AzResourceGroup -Name $rgName -Location $resourceGroupLocation

    # Validar la operación
    Validate-Operation "Creating Resource Group" $rgName
}

Write-Host "Se definen las variables de los servicios.";

$accountName = "storageblobs$suffix"
$containerName = "containerbacpac$suffix"
$containerName2 = "landing$suffix"
$location = "East US"
$rolqueue = "Storage Queue Data Contributor"
$rolblobs = "Storage Blob Data Contributor"


$tenandID='2f4a9838-26b7-47ee-be60-ccc1fdec5953'



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

#-------------CREAR STORAGE ACCOUNT, CONTAINER Y SUBIR BACPAC------------

Write-Host "Storage Account...";
$StorageAccount = New-AzStorageAccount -ResourceGroupName $rgName `
          -Name $accountName `
          -SkuName Standard_RAGRS `
          -Location $location `
          -Kind StorageV2 

$StorageAccount
#-------------rol--------------

Write-Host "Storage Account...";


Write-Host "Roles de Storage Container...";
New-AzStorageContainer -Name $containerName -Context $StorageAccount.Context -Permission blob
New-AzStorageContainer -Name $containerName2 -Context $StorageAccount.Context -Permission Container


# creo un storage account para el datalake con accedo jerarquico
Write-host "Creo el Data lake..."
$azureDataLake = New-AzStorageAccount -ResourceGroupName $rgName `
                                      -Name $DataLakeName `
                                      -Location $location `
                                      -SkuName Standard_RAGRS `
                                      -Kind StorageV2 `
                                      -EnableHierarchicalNamespace $True
$azureDataLake

Write-host "Agrego Permisos al contex de $azureDataLake..."

New-AzStorageContainer -Name $filesystemName -Context $azureDataLake.context -Permission Blob


# extraigo claves de las dos storage accounts
$storageAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $rgName -Name $accountName).Value[0]

$dataLakeAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $rgName -Name $DataLakeName).Value[0]



#Subo el bacpac

Write-Host "Deploy bacpac...";
$StorageBlobContent = @{
  File             = "$(Get-Location)\$namedbbacpac"
  Container        = $containerName
  Blob             = $namedbbacpac
  Context          = $storageAccount.context
  StandardBlobTier = 'Hot'
}

$Estatus_deploy = Set-AzStorageBlobContent @StorageBlobContent

Write-Host "Usted subio el bacpac el dia:" 
$Estatus_deploy.LastModified.Date



$storageKey = Get-AzStorageAccountKey -ResourceGroupName $rgName -Name $accountName

Write-Host "Deploy Server SQL...";

$server = New-AzSqlServer -ResourceGroupName $rgName `
    -ServerName $serverName `
    -Location $location `
    -SqlAdministratorCredentials $(New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $adminSqlLogin, $(ConvertTo-SecureString -String $adminSqlPassword -AsPlainText -Force))

$server



Write-Host "Rule de Firewail...";
# FIREWALL RULE
$myPublicIp = (Invoke-WebRequest -Uri "http://ipinfo.io/ip" -UseBasicParsing).content


$IP = New-AzSqlServerFirewallRule -ResourceGroupName $rgName -ServerName $serverName -AllowAllAzureIPs
New-AzSqlServerFirewallRule -ResourceGroupName $rgName -ServerName $serverName -FirewallRuleName "AllowMyPublicIP" -StartIpAddress $IP.StartIpAddress -EndIpAddress $myPublicIP

#CONVERT STRING TO SECURE STRING

$adminSqlPasswordSecure = ConvertTo-SecureString $adminSqlPassword -AsPlainText -Force

Write-Host "Creando base de dato...";
#Sql
$database = New-AzSqlDatabase -ComputeGeneration Gen5 `
                                -DatabaseName $databaseName `
                                -Edition GeneralPurpose `
                                -ResourceGroupName $rgName `
                                -ServerName $server.ServerName `
                                -VCore 2
$database

# Importamos Bacpac.
Write-Host "Importando el bacpac a base de dato, calma tomara unos minutos...";
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
                -DatabaseMaxSizeBytes 100GB

Write-Host "Importacion Terminada..";

$Statusimport
  
Write-Host "Creating $databricks Azure Databricks workspace in $rgName resource group..."

New-AzResourceGroupDeployment -ResourceGroupName $rgName -TemplateFile ./databricks/main.bicep -workspaceName $databricks

$databrick = New-AzDatabricksWorkspace `
                -Name $databricks `
                -ResourceGroupName $rgName `
                -Location $location `
                -Sku standard | Out-Null

$databrick

Write-host "Creando DataFactory V2..."

$adf = Set-AzDataFactoryV2 -ResourceGroupName $rgName -Name $dataFactoryName -Location $location

Write-Host "Status datafactory :"$adf.ProvisioningState
$adf
Write-Host "LinkService a SQl Database...";

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


Write-Host "LinkService a SQl Database...";
#BLOB STORAGE LINKED SERVICES
$azureSQLDatabaseLinkedServiceDefinition | Out-File ".\$azureSqlDatabaseLinkedService.json"
Set-AzDataFactoryV2LinkedService -DataFactoryName $dataFactoryName -ResourceGroupName $rgName -Name "AzureSqlDatabaseLinkedService" -DefinitionFile ".\$azureSqlDatabaseLinkedService.json"


Write-Host "LinkService a Blob Storage...";
#BLOB STORAGE LINKED SERVICES
$storageKey = Get-AzStorageAccountKey -ResourceGroupName $rgName -Name $accountName

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
#DATABRICKS LINKED SERVICES

Write-Host "LinkService Databrick...";
$azureStorageLinkedServiceDefinition | Out-File ".\$azureStorageLinkedService.json"
Set-AzDataFactoryV2LinkedService -DataFactoryName $dataFactoryName -ResourceGroupName $rgName -Name "AzureStorageLinkedService" -DefinitionFile ".\$azureStorageLinkedService.json"



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

$azurDatabricksLinkedServiceDefinition | Out-File ".\$azurDatabricksLinkedService.json"
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