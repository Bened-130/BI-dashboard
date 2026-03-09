from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Iterator, Dict, Any
import logging
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Abstract base class for data extractors"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    @abstractmethod
    def extract(self, **kwargs) -> DataFrame:
        pass
    
    def extract_incremental(
        self, 
        watermark_column: str, 
        last_watermark: str,
        **kwargs
    ) -> DataFrame:
        """Extract data incrementally based on watermark"""
        df = self.extract(**kwargs)
        return df.filter(col(watermark_column) > last_watermark)


class SynapseSQLExtractor(BaseExtractor):
    """Extractor for Synapse SQL pools using JDBC"""
    
    def __init__(
        self, 
        spark: SparkSession,
        jdbc_url: str,
        credential: DefaultAzureCredential
    ):
        super().__init__(spark)
        self.jdbc_url = jdbc_url
        self.credential = credential
        self.token = self._get_access_token()
    
    def _get_access_token(self) -> str:
        """Get AAD token for SQL authentication"""
        token = self.credential.get_token("https://database.windows.net/.default")
        return token.token
    
    def extract(self, query: str, **kwargs) -> DataFrame:
        """Extract using SQL query"""
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("query", query) \
            .option("accessToken", self.token) \
            .load()
    
    def extract_partitioned(
        self,
        query: str,
        partition_column: str,
        num_partitions: int,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None
    ) -> DataFrame:
        """Parallel extraction using partitioning"""
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("query", query) \
            .option("accessToken", self.token) \
            .option("partitionColumn", partition_column) \
            .option("numPartitions", num_partitions) \
            .option("lowerBound", lower_bound or 0) \
            .option("upperBound", upper_bound or 1000000) \
            .load()


class DataverseExtractor(BaseExtractor):
    """Extractor for Microsoft Dataverse/CDS"""
    
    def __init__(self, spark: SparkSession, environment_url: str):
        super().__init__(spark)
        self.environment_url = environment_url
    
    def extract(self, entity_name: str, **kwargs) -> DataFrame:
        """Extract Dataverse entity using Synapse Link or API"""
        # Using Synapse Link for Dataverse (preferred)
        path = f"abfss://dataverse@{self.environment_url}.dfs.core.windows.net/{entity_name}"
        
        return self.spark.read \
            .format("parquet") \
            .load(path)


class EventHubExtractor(BaseExtractor):
    """Real-time extractor from Event Hubs"""
    
    def __init__(self, spark: SparkSession, connection_string: str):
        super().__init__(spark)
        self.connection_string = connection_string
    
    def extract_stream(self, consumer_group: str = "$Default") -> DataFrame:
        """Create streaming DataFrame from Event Hub"""
        eh_conf = {
            'eventhubs.connectionString': self.connection_string,
            'eventhubs.consumerGroup': consumer_group,
            'eventhubs.startingPosition': '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}'
        }
        
        return self.spark.readStream \
            .format("eventhubs") \
            .options(**eh_conf) \
            .load()


class RESTAPIExtractor(BaseExtractor):
    """Generic REST API extractor with pagination"""
    
    def __init__(self, spark: SparkSession, base_url: str):
        super().__init__(spark)
        self.base_url = base_url
    
    def extract(
        self,
        endpoint: str,
        headers: Optional[Dict[str, str]] = None,
        pagination_key: Optional[str] = None
    ) -> DataFrame:
        """Extract from paginated REST API"""
        import requests
        import pandas as pd
        
        all_data = []
        url = f"{self.base_url}/{endpoint}"
        
        while url:
            response = requests.get(url, headers=headers)
            data = response.json()
            all_data.extend(data.get("results", data))
            
            # Handle pagination
            url = data.get(pagination_key) if pagination_key else None
        
        # Convert to Spark DataFrame
        pandas_df = pd.json_normalize(all_data)
        return self.spark.createDataFrame(pandas_df)