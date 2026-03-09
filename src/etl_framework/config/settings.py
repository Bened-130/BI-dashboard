from pydantic import BaseSettings, Field, validator
from typing import Optional, Dict, Any
from functools import lru_cache
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


class SynapseSettings(BaseSettings):
    """Azure Synapse specific settings"""
    workspace_name: str = Field(..., env="SYNAPSE_WORKSPACE_NAME")
    spark_pool_name: str = Field(..., env="SPARK_POOL_NAME")
    sql_pool_name: Optional[str] = Field(None, env="DEDICATED_SQL_POOL_NAME")
    subscription_id: str = Field(..., env="AZURE_SUBSCRIPTION_ID")
    resource_group: str = Field(..., env="RESOURCE_GROUP")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


class StorageSettings(BaseSettings):
    """ADLS Gen2 settings"""
    account_name: str = Field(..., env="ADLS_ACCOUNT_NAME")
    filesystem_name: str = Field("data", env="ADLS_FILESYSTEM")
    bronze_path: str = "bronze"
    silver_path: str = "silver"
    gold_path: str = "gold"
    
    @property
    def account_url(self) -> str:
        return f"https://{self.account_name}.dfs.core.windows.net"
    
    @property
    def abfss_root(self) -> str:
        return f"abfss://{self.filesystem_name}@{self.account_name}.dfs.core.windows.net"


class DeltaSettings(BaseSettings):
    """Delta Lake configuration"""
    enable_optimize_write: bool = True
    enable_auto_compact: bool = True
    checkpoint_interval: int = 10
    retention_duration: str = "7 days"


class MonitoringSettings(BaseSettings):
    """Application Insights and logging"""
    app_insights_connection_string: Optional[str] = Field(None, env="APP_INSIGHTS_CONNECTION_STRING")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    enable_distributed_tracing: bool = True


class Settings(BaseSettings):
    """Master configuration"""
    environment: str = Field("dev", env="ENVIRONMENT")
    synapse: SynapseSettings = SynapseSettings()
    storage: StorageSettings = StorageSettings()
    delta: DeltaSettings = DeltaSettings()
    monitoring: MonitoringSettings = MonitoringSettings()
    
    class Config:
        env_file = ".env"
        
    def get_credential(self):
        """Get Azure credential"""
        return DefaultAzureCredential()


@lru_cache()
def get_settings() -> Settings:
    """Cached settings instance"""
    return Settings()


# Key Vault integration for secrets
class SecretManager:
    """Manage secrets from Azure Key Vault"""
    
    def __init__(self, vault_url: str):
        self.credential = DefaultAzureCredential()
        self.client = SecretClient(vault_url=vault_url, credential=self.credential)
    
    def get_secret(self, name: str) -> str:
        return self.client.get_secret(name).value
    
    def get_db_connection_string(self, db_name: str) -> str:
        """Retrieve connection string from Key Vault"""
        secret_name = f"{db_name}-connection-string"
        return self.get_secret(secret_name)