import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

# PySpark imports with graceful fallback
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, current_timestamp, lit
    from delta.tables import DeltaTable
    DELTA_AVAILABLE = True
except ImportError:
    DELTA_AVAILABLE = False
    SparkSession = None
    DataFrame = None
    DeltaTable = None

logger = logging.getLogger(__name__)


class DeltaLakeManager:
    """Enterprise Delta Lake manager for Synapse Spark"""
    
    def __init__(self, spark: Any, storage_path: str):
        """
        Initialize Delta Lake manager
        
        Args:
            spark: SparkSession instance
            storage_path: Root storage path (abfss://...)
        """
        if not DELTA_AVAILABLE:
            raise ImportError("PySpark or Delta Lake not available. Install with: pip install pyspark delta-spark")
        
        self.spark = spark
        self.storage_path = storage_path
        self._configure_delta()
    
    def _configure_delta(self):
        """Configure Spark for Delta Lake"""
        self.spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        self.spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        self.spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
        self.spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
        self.spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    
    def write_bronze(
        self, 
        df: Any, 
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        mode: str = "overwrite"
    ) -> None:
        """
        Write raw data to Bronze layer with metadata
        
        Args:
            df: Input DataFrame
            table_name: Target table name
            partition_cols: Columns to partition by
            mode: Write mode (overwrite/append)
        """
        path = f"{self.storage_path}/bronze/{table_name}"
        
        # Add metadata columns
        df_with_meta = df \
            .withColumn("_ingestion_timestamp", current_timestamp()) \
            .withColumn("_source_system", lit("source_db")) \
            .withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d%H%M%S")))
        
        writer = df_with_meta.write \
            .format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer.save(path)
        logger.info(f"Written {df.count()} records to Bronze: {path}")
    
    def write_silver(
        self,
        df: Any,
        table_name: str,
        merge_key: Optional[str] = None,
        partition_cols: Optional[List[str]] = None
    ) -> None:
        """
        Write cleaned data to Silver layer with merge support
        
        Args:
            df: Transformed DataFrame
            table_name: Target table name
            merge_key: Key for merge operations (comma-separated for composite keys)
            partition_cols: Partition columns
        """
        path = f"{self.storage_path}/silver/{table_name}"
        
        if merge_key and self._table_exists(path):
            # Perform merge (SCD Type 1)
            self._merge_delta_table(df, path, merge_key)
        else:
            writer = df.write.format("delta").mode("overwrite")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(path)
        
        logger.info(f"Written to Silver: {path}")
    
    def _merge_delta_table(
        self, 
        df: Any, 
        path: str, 
        merge_key: str
    ) -> None:
        """Perform Delta merge operation"""
        delta_table = DeltaTable.forPath(self.spark, path)
        
        # Build merge condition dynamically
        keys = [k.strip() for k in merge_key.split(",")]
        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in keys])
        
        delta_table.alias("target") \
            .merge(df.alias("source"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    
    def write_gold(
        self,
        df: Any,
        table_name: str,
        distribution_type: str = "ROUND_ROBIN",
        index_cols: Optional[List[str]] = None
    ) -> None:
        """
        Write aggregated data to Gold layer
        
        Args:
            df: Aggregated DataFrame
            table_name: Target table name
            distribution_type: Hash, Round_Robin, or Replicate
            index_cols: Columns for indexing
        """
        # Write to external table location first
        path = f"{self.storage_path}/gold/{table_name}"
        df.write.format("delta").mode("overwrite").save(path)
        
        # Create external table in Dedicated SQL Pool
        self._create_external_table(table_name, path, distribution_type, index_cols)
    
    def _create_external_table(
        self,
        table_name: str,
        data_path: str,
        distribution_type: str,
        index_cols: Optional[List[str]]
    ) -> None:
        """Create external table in Synapse Dedicated SQL Pool"""
        # This would use JDBC to execute DDL
        ddl = f"""
        CREATE EXTERNAL TABLE {table_name}
        WITH (
            LOCATION = '{data_path}',
            FILE_FORMAT = 'DeltaFormat',
            REJECT_TYPE = VALUE,
            REJECT_VALUE = 0
        )
        DISTRIBUTION = {distribution_type}
        """
        logger.info(f"Created external table DDL: {ddl}")
    
    def _table_exists(self, path: str) -> bool:
        """Check if Delta table exists"""
        try:
            DeltaTable.forPath(self.spark, path)
            return True
        except Exception:
            return False
    
    def optimize_table(self, table_name: str, zorder_cols: Optional[List[str]] = None) -> None:
        """Optimize Delta table with VACUUM and OPTIMIZE"""
        path = f"{self.storage_path}/silver/{table_name}"
        delta_table = DeltaTable.forPath(self.spark, path)
        
        # Run OPTIMIZE (compaction)
        if zorder_cols:
            delta_table.optimize().executeZOrderBy(*zorder_cols)
        else:
            delta_table.optimize().executeCompaction()
        
        # Run VACUUM (cleanup old versions)
        self.spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")
        
        logger.info(f"Optimized table: {table_name}")
    
    def get_table_history(self, table_name: str, layer: str = "silver") -> Any:
        """Get Delta table version history"""
        path = f"{self.storage_path}/{layer}/{table_name}"
        delta_table = DeltaTable.forPath(self.spark, path)
        return delta_table.history()
    
    def time_travel_query(self, table_name: str, timestamp: str, layer: str = "silver") -> Any:
        """Query table at specific point in time"""
        path = f"{self.storage_path}/{layer}/{table_name}"
        return self.spark.read \
            .format("delta") \
            .option("timestampAsOf", timestamp) \
            .load(path)