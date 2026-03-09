"""
Azure Synapse ETL Framework
Production-grade data engineering framework for Azure Synapse Analytics
"""

__version__ = "2.1.0"
__author__ = "Data Engineering Team"

from .config.settings import get_settings, Settings
from .connectors.delta_lake_manager import DeltaLakeManager
from .core.extractors import SynapseSQLExtractor

__all__ = [
    "get_settings",
    "Settings",
    "DeltaLakeManager",
    "SynapseSQLExtractor",
]