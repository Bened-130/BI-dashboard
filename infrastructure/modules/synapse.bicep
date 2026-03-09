@description('Synapse workspace name')
param name string

@description('Azure region')
param location string = resourceGroup().location

@description('Storage account name for default data lake')
param storageAccountName string

@description('SQL admin username')
param sqlAdminUsername string = 'sqladminuser'

@description('SQL admin password')
@secure()
param sqlAdminPassword string

@description('Log Analytics workspace ID for diagnostics')
param logAnalyticsWorkspaceId string

resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: storageAccountName
}

resource synapse 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: name
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    defaultDataLakeStorage: {
      accountUrl: 'https://${storage.name}.dfs.core.windows.net'
      filesystem: 'data'
    }
    sqlAdministratorLogin: sqlAdminUsername
    sqlAdministratorLoginPassword: sqlAdminPassword
    managedVirtualNetwork: 'default'
    publicNetworkAccess: 'Enabled'
  }
}

output synapseId string = synapse.id
output synapseName string = synapse.name
output synapsePrincipalId string = synapse.identity.principalId
