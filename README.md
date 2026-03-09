# Azure Synapse ETL Framework

Production-grade ETL framework for Azure Synapse Analytics implementing the Medallion architecture.

## Quick Start

### Prerequisites
- Azure CLI
- Python 3.10+
- Azure subscription with Owner access

### 1. Infrastructure Deployment

```bash
# Login to Azure
az login

# Create resource group
az group create --name rg-synapse-etl-prod --location eastus

# Deploy infrastructure
az deployment group create \
  --resource-group rg-synapse-etl-prod \
  --template-file infrastructure/main.bicep \
  --parameters environment=prod sqlAdminPassword=YourSecurePassword123!