# Azure Synapse ETL Framework

Production-grade ETL framework for Azure Synapse Analytics implementing the Medallion architecture.

## Quick Start

### Prerequisites
- Azure CLI
- Python 3.10+
- Terraform or Azure Bicep
- Azure subscription with Owner access

### 1. Infrastructure Deployment

```bash
# Login to Azure
az login

# Deploy infrastructure
cd infrastructure
az deployment group create \
  --resource-group rg-synapse-etl-prod \
  --template-file main.bicep \
  --parameters environment=prod