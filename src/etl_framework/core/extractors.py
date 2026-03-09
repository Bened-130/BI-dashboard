import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Iterator

# Optional imports with graceful fallback
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = None
    DataFrame = None
    col = None

try:
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    DefaultAzureCredential = None
    DataLakeServiceClient = None

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    requests = None

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Abstract base class for data extractors"""
    
    def __init__(self, spark: Any):
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark not available")
        self.spark = spark
    
    @abstractmethod
    def extract(self, **kwargs) -> Any:
        """Extract data and return DataFrame"""
        pass
    
    def extract_incremental(
        self, 
        watermark_column: str, 
        last_watermark: str,
        **kwargs
    ) -> Any:
        """Extract data incrementally based on watermark"""
        df = self.extract(**kwargs)
        return df.filter(col(watermark_column) > last_watermark)


class SynapseSQLExtractor(BaseExtractor):
    """Extractor for Synapse SQL pools using JDBC"""
    
    def __init__(
        self, 
        spark: Any,
        jdbc_url: str,
        credential: Any = None
    ):
        super().__init__(spark)
        self.jdbc_url = jdbc_url
        self.credential = credential or (DefaultAzureCredential() if AZURE_AVAILABLE else None)
        self.token = self._get_access_token() if self.credential else None
    
    def _get_access_token(self) -> str:
        """Get AAD token for SQL authentication"""
        if not AZURE_AVAILABLE:
            raise ImportError("Azure Identity not available")
        token = self.credential.get_token("https://database.windows.net/.default")
        return token.token
    
    def extract(self, query: str, **kwargs) -> Any:
        """Extract using SQL query"""
        reader = self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("query", query)
        
        if self.token:
            reader = reader.option("accessToken", self.token)
        else:
            # Fallback to SQL auth if no token
            reader = reader.option("user", kwargs.get("user", "")).option("password", kwargs.get("password", ""))
        
        return reader.load()
    
    def extract_partitioned(
        self,
        query: str,
        partition_column: str,
        num_partitions: int,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None
    ) -> Any:
        """Parallel extraction using partitioning"""
        reader = self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("query", query) \
            .option("partitionColumn", partition_column) \
            .option("numPartitions", num_partitions) \
            .option("lowerBound", lower_bound or 0) \
            .option("upperBound", upper_bound or 1000000)
        
        if self.token:
            reader = reader.option("accessToken", self.token)
        
        return reader.load()


class DataverseExtractor(BaseExtractor):
    """Extractor for Microsoft Dataverse/CDS"""
    
    def __init__(self, spark: Any, environment_url: str):
        super().__init__(spark)
        self.environment_url = environment_url
    
    def extract(self, entity_name: str, **kwargs) -> Any:
        """Extract Dataverse entity using Synapse Link or API"""
        # Using Synapse Link for Dataverse (preferred)
        path = f"abfss://dataverse@{self.environment_url}.dfs.core.windows.net/{entity_name}"
        
        return self.spark.read \
            .format("parquet") \
            .load(path)


class EventHubExtractor(BaseExtractor):
    """Real-time extractor from Event Hubs"""
    
    def __init__(self, spark: Any, connection_string: str):
        super().__init__(spark)
        self.connection_string = connection_string
    
    def extract_stream(self, consumer_group: str = "$Default") -> Any:
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
    
    def __init__(self, spark: Any, base_url: str):
        super().__init__(spark)
        self.base_url = base_url
    
    def extract(
        self,
        endpoint: str,
        headers: Optional[Dict[str, str]] = None,
        pagination_key: Optional[str] = None
    ) -> Any:
        """Extract from paginated REST API"""
        if not REQUESTS_AVAILABLE:
            raise ImportError("requests library not installed. Install with: pip install requests")
        
        import pandas as pd
        
        all_data = []
        url = f"{self.base_url}/{endpoint}"
        
        while url:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            all_data.extend(data.get("results", data))
            
            # Handle pagination
            url = data.get(pagination_key) if pagination_key else None
        
        # Convert to Spark DataFrame
        pandas_df = pd.json_normalize(all_data)
        return self.spark.createDataFrame(pandas_df)