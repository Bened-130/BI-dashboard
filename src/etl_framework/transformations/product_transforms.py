from __future__ import annotations
import logging
from typing import Any, List, Optional, Dict, Union  # FIXED: Ensure Any is imported here

# PySpark imports with graceful fallback
try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import (
        col, when, lit, trim, initcap, upper, 
        regexp_replace, round as spark_round,
        current_timestamp, input_file_name, length,
        datediff, month
    )
    from pyspark.sql.types import StringType, DoubleType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = None

try:
    import pandas as pd
    import numpy as np
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None
    np = None

logger = logging.getLogger(__name__)


class ProductTransformer:
    """Enterprise-grade product data transformations"""
    
    def __init__(self):
        self.validation_rules: List[str] = []
    
    def standardize_schema(self, df: Any) -> Any:
        """Standardize column names and types"""
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark not available")
        
        # Rename columns to snake_case
        for col_name in df.columns:
            new_name = col_name.lower().replace(" ", "_").replace("-", "_")
            df = df.withColumnRenamed(col_name, new_name)
        
        # Cast types
        type_mapping = {
            "product_id": "string",
            "price": "decimal(10,2)",
            "created_date": "timestamp",
            "last_updated": "timestamp"
        }
        
        for col_name, data_type in type_mapping.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(data_type))
        
        return df
    
    def clean_text_fields(self, df: Any) -> Any:
        """Clean and standardize text fields"""
        text_columns = ["product_name", "category", "description"]
        
        for col_name in text_columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    initcap(trim(regexp_replace(col(col_name), r"\s+", " ")))
                )
        
        # Standardize category to uppercase
        if "category" in df.columns:
            df = df.withColumn("category", upper(col("category")))
        
        return df
    
    def handle_missing_values(self, df: Any) -> Any:
        """Handle missing values with business rules"""
        # Fill missing categories
        df = df.fillna({"category": "UNCATEGORIZED"})
        
        # Fill missing prices with median (computed dynamically)
        median_price = df.approxQuantile("price", [0.5], 0.01)[0]
        df = df.fillna({"price": median_price})
        
        # Flag records with imputed values
        df = df.withColumn(
            "_has_imputed_values",
            when(
                (col("category") == "UNCATEGORIZED") | 
                (col("price") == median_price),
                True
            ).otherwise(False)
        )
        
        return df
    
    def add_derived_columns(self, df: Any) -> Any:
        """Add business-derived columns"""
        # Price tiers
        df = df.withColumn(
            "price_tier",
            when(col("price") < 50, "Budget")
            .when(col("price") < 100, "Standard")
            .when(col("price") < 500, "Premium")
            .otherwise("Luxury")
        )
        
        # Product age
        df = df.withColumn(
            "product_age_days",
            datediff(current_timestamp(), col("created_date"))
        )
        
        # Seasonality flag
        df = df.withColumn(
            "season",
            when(month(col("created_date")).isin([12, 1, 2]), "Winter")
            .when(month(col("created_date")).isin([3, 4, 5]), "Spring")
            .when(month(col("created_date")).isin([6, 7, 8]), "Summer")
            .otherwise("Fall")
        )
        
        return df
    
    def calculate_price_score(self, df: Any) -> Any:
        """Calculate price competitiveness score"""
        if not PANDAS_AVAILABLE:
            logger.warning("Pandas not available, skipping price score calculation")
            return df.withColumn("price_competitiveness_score", lit(50.0))
        
        # Collect prices to driver for calculation (for small datasets)
        prices = [row.price for row in df.select("price").collect()]
        
        if not prices or len(prices) == 0:
            return df.withColumn("price_competitiveness_score", lit(50.0))
        
        mean_price = sum(prices) / len(prices)
        std_price = (sum((x - mean_price) ** 2 for x in prices) / len(prices)) ** 0.5
        
        if std_price == 0:
            return df.withColumn("price_competitiveness_score", lit(50.0))
        
        # Calculate z-score and convert to 0-100 scale
        df = df.withColumn(
            "price_competitiveness_score",
            when(
                std_price != 0,
                spark_round(
                    50 + ((col("price") - lit(mean_price)) / lit(std_price)) * 10,
                    2
                )
            ).otherwise(50.0)
        )
        
        # Clip to 0-100 range
        df = df.withColumn(
            "price_competitiveness_score",
            when(col("price_competitiveness_score") < 0, 0.0)
            .when(col("price_competitiveness_score") > 100, 100.0)
            .otherwise(col("price_competitiveness_score"))
        )
        
        return df
    
    def apply_ml_features(self, df: Any) -> Any:
        """Add ML-ready features"""
        # Add price score
        df = self.calculate_price_score(df)
        
        # Add embedding placeholder (would use actual ML model)
        df = df.withColumn("product_embedding", lit(None).cast("array<float>"))
        
        return df
    
    def validate_business_rules(self, df: Any) -> Any:
        """Apply business rule validations"""
        # Flag invalid records
        df = df.withColumn(
            "_is_valid",
            when(
                (col("price") > 0) & 
                (col("product_name").isNotNull()) &
                (length(col("product_name")) > 2),
                True
            ).otherwise(False)
        )
        
        # Separate valid and invalid
        valid_df = df.filter(col("_is_valid") == True)
        invalid_df = df.filter(col("_is_valid") == False)
        
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            logger.warning(f"Found {invalid_count} invalid product records")
            # Write to quarantine
            invalid_df.write.mode("append").parquet("/mnt/quarantine/products")
        
        return valid_df
    
    def transform(self, df: Any) -> Any:
        """Execute full transformation pipeline"""
        result = self.standardize_schema(df)
        result = self.clean_text_fields(result)
        result = self.handle_missing_values(result)
        result = self.add_derived_columns(result)
        result = self.apply_ml_features(result)
        result = self.validate_business_rules(result)
        return result