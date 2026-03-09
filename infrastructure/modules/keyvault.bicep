@description('Key Vault name')
param name string

@description('Azure region')
param location string = resourceGroup().location

@description('Tenant ID')
param tenantId string = subscription().tenantId

@description('Enable for template deployment')
param enabledForTemplateDeployment bool = true

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: name
  location: location
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: tenantId
    accessPolicies: []
    enabledForTemplateDeployment: enabledForTemplateDeployment
    enabledForDiskEncryption: true
    enabledForDeployment: true
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
  }
}

output keyVaultId string = keyVault.id
output keyVaultName string = keyVault.name
