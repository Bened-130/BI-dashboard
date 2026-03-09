@description('Environment name')
param environment string = 'dev'

@description('Azure region')
param location string = resourceGroup().location

@description('Project name')
param projectName string = 'synapseetl'

// Variables
var baseName = '${projectName}-${environment}'
var synapseName = '${baseName}-synapse'
var storageName = '${baseName}adls'

// Storage Account with Hierarchical Namespace
resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageName
  location: location
  sku: {
    name: 'Standard_GRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    networkAcls: {
      defaultAction: 'Deny'
      bypass: 'AzureServices'
    }
  }
}

// Synapse Workspace
resource synapse 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: synapseName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    defaultDataLakeStorage: {
      accountUrl: 'https://${storage.name}.dfs.core.windows.net'
      filesystem: 'data'
    }
    sqlAdministratorLogin: 'sqladminuser'
    sqlAdministratorLoginPassword: keyVault.getSecret('synapse-sql-password')
    managedVirtualNetwork: 'default'
    publicNetworkAccess: 'Enabled'
  }
}

// Spark Pool with Auto-Scale
resource sparkPool 'Microsoft.Synapse/workspaces/bigDataPools@2021-06-01' = {
  parent: synapse
  name: 'etlsparkpool'
  location: location
  properties: {
    nodeSizeFamily: 'MemoryOptimized'
    nodeSize: 'Medium'
    autoScale: {
      enabled: true
      minNodeCount: 3
      maxNodeCount: 50
    }
    dynamicExecutorAllocation: {
      enabled: true
      minExecutors: 2
      maxExecutors: 40
    }
    sparkVersion: '3.4'
    sessionLevelPackagesEnabled: true
    customLibraries: [
      {
        type: 'PyPi'
        name: 'great_expectations'
        version: '0.18.0'
      }
      {
        type: 'PyPi'
        name: 'delta-spark'
        version: '2.4.0'
      }
    ]
  }
}

// Dedicated SQL Pool (Gen2)
resource sqlPool 'Microsoft.Synapse/workspaces/sqlPools@2021-06-01' = {
  parent: synapse
  name: 'dedicatedpool'
  location: location
  sku: {
    name: 'DW100c'
    tier: 'DataWarehouse'
  }
  properties: {
    createMode: 'Default'
    collation: 'SQL_Latin1_General_CP1_CI_AS'
  }
}

// Key Vault for Secrets
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: '${baseName}-kv'
}

// Application Insights for Monitoring
resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: '${baseName}-appinsights'
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: logAnalytics.id
  }
}

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: '${baseName}-loganalytics'
  location: location
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

// Outputs
output synapseWorkspaceName string = synapse.name
output storageAccountName string = storage.name
output sparkPoolName string = sparkPool.name
