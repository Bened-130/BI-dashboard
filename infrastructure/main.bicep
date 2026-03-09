@description('Environment name')
@allowed(['dev', 'staging', 'prod'])
param environment string = 'dev'

@description('Azure region')
param location string = resourceGroup().location

@description('Project name')
param projectName string = 'synapseetl'

@description('SQL Admin Password')
@secure()
param sqlAdminPassword string

// Variables
var baseName = '${projectName}${environment}'
var synapseName = '${baseName}synapse'
var storageName = '${baseName}adls'
var keyVaultName = '${baseName}kv'

// Reference existing Key Vault or create new
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    accessPolicies: []
    enabledForTemplateDeployment: true
    enabledForDiskEncryption: true
    enabledForDeployment: true
    enableRbacAuthorization: true
  }
}

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
    allowBlobPublicAccess: false
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
}

// Storage Container
resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: '${storageName}/default/data'
  dependsOn: [
    storage
  ]
}

// Log Analytics Workspace
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

// Application Insights
resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: '${baseName}-appinsights'
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: logAnalytics.id
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
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
    sqlAdministratorLoginPassword: sqlAdminPassword
    managedVirtualNetwork: 'default'
    publicNetworkAccess: 'Enabled'
    azureADOnlyAuthentication: false
    trustedServiceBypassEnabled: true
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
    customLibraries: []
    isComputeIsolationEnabled: false
  }
}

// Dedicated SQL Pool
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

// Role Assignment for Synapse to Storage
resource synapseStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(synapse.id, storage.id, 'StorageBlobDataContributor')
  scope: storage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe') // Storage Blob Data Contributor
    principalId: synapse.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// Outputs
output synapseWorkspaceName string = synapse.name
output synapseWorkspaceId string = synapse.id
output storageAccountName string = storage.name
output keyVaultName string = keyVault.name
output sparkPoolName string = sparkPool.name
output sqlPoolName string = sqlPool.name
output appInsightsName string = appInsights.name
output logAnalyticsWorkspaceId string = logAnalytics.id
