from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, trim, initcap, upper, 
    regexp_replace, round as spark_round,
    current_timestamp, input_file_name,
    monotonically_increasing_id,
    pandas_udf, PandasUDFType
)
from pyspark.sql.types import StringType, DoubleType
import pandas as pd
import numpy as np
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class ProductTransformer:
    """Enterprise-grade product data transformations"""
    
    def __init__(self):
        self.validation_rules = []
    
    def standardize_schema(self, df: DataFrame) -> DataFrame:
        """Standardize column names and types"""
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
    
    def clean_text_fields(self, df: DataFrame) -> DataFrame:
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
    
    def handle_missing_values(self, df: DataFrame) -> DataFrame:
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
    
    def add_derived_columns(self, df: DataFrame) -> DataFrame:
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
    
    @pandas_udf(DoubleType(), PandasUDFType.SCALAR)
    def calculate_price_score(prices: pd.Series) -> pd.Series:
        """Pandas UDF for complex price scoring using numpy"""
        # Z-score normalization
        mean_price = prices.mean()
        std_price = prices.std()
        
        if std_price == 0:
            return pd.Series([50.0] * len(prices))
        
        z_scores = (prices - mean_price) / std_price
        # Convert to 0-100 scale
        scores = 50 + (z_scores * 10)
        return np.clip(scores, 0, 100)
    
    def apply_ml_features(self, df: DataFrame) -> DataFrame:
        """Add ML-ready features"""
        # Apply pandas UDF
        df = df.withColumn(
            "price_competitiveness_score",
            self.calculate_price_score(col("price"))
        )
        
        # Add embedding placeholder (would use actual ML model)
        df = df.withColumn("product_embedding", lit(None).cast("array<float>"))
        
        return df
    
    def validate_business_rules(self, df: DataFrame) -> DataFrame:
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
        
        if invalid_df.count() > 0:
            logger.warning(f"Found {invalid_df.count()} invalid product records")
            # Write to quarantine
            invalid_df.write.mode("append").parquet("/mnt/quarantine/products")
        
        return valid_df
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Execute full transformation pipeline"""
        return (
            df
            .transform(self.standardize_schema)
            .transform(self.clean_text_fields)
            .transform(self.handle_missing_values)
            .transform(self.add_derived_columns)
            .transform(self.apply_ml_features)
            .transform(self.validate_business_rules)
        )