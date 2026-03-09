import os
import sys

# Set local Spark environment
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import pandas as pd


def create_sample_data():
    """Create sample product data for testing"""
    print("Creating sample data...")
    
    data = {
        'product_id': ['PROD-000001', 'PROD-000002', 'PROD-000003', 'PROD-000004'],
        'product_name': ['  wireless mouse  ', 'Gaming KEYBOARD', 'usb-C cable', None],
        'category': ['electronics', 'Electronics', 'Accessories', 'Electronics'],
        'price': [29.99, 89.50, 15.00, None],
        'created_date': ['2024-01-15', '2024-02-20', '2024-03-10', '2024-01-05'],
        'last_updated': ['2024-03-01', '2024-03-05', '2024-03-08', '2024-03-10']
    }
    
    return pd.DataFrame(data)


def run_local_etl():
    """Execute ETL pipeline locally"""
    print("\n" + "="*70)
    print("LOCAL SPARK ETL TEST")
    print("="*70)
    
    # Initialize Spark
    print("\n[1/6] Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("LocalETLTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("   ✓ Spark session created")
    
    # Create sample data
    print("\n[2/6] Loading sample data...")
    pandas_df = create_sample_data()
    bronze_df = spark.createDataFrame(pandas_df)
    print(f"   ✓ Loaded {bronze_df.count()} records")
    print("   Sample data:")
    bronze_df.show(truncate=False)
    
    # Import and run transformations
    print("\n[3/6] Running transformations...")
    sys.path.insert(0, 'src')
    
    try:
        from etl_framework.transformations.product_transforms import ProductTransformer
        
        transformer = ProductTransformer()
        
        # Apply transformations step by step
        print("   → Standardizing schema...")
        df = transformer.standardize_schema(bronze_df)
        
        print("   → Cleaning text fields...")
        df = transformer.clean_text_fields(df)
        
        print("   → Handling missing values...")
        df = transformer.handle_missing_values(df)
        
        print("   → Adding derived columns...")
        df = transformer.add_derived_columns(df)
        
        print("   ✓ Transformations complete")
        
    except Exception as e:
        print(f"   ✗ Transformation error: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Show results
    print("\n[4/6] Transformation results:")
    print(f"   Records processed: {df.count()}")
    print("   Sample output:")
    df.select("product_id", "product_name", "category", "price", 
              "price_tier", "season", "_has_imputed_values").show(truncate=False)
    
    # Write to local Delta (simulated)
    print("\n[5/6] Writing to local Delta format...")
    output_path = "data/local_test/silver/products"
    os.makedirs(output_path, exist_ok=True)
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(output_path)
    
    print(f"   ✓ Data written to: {output_path}")
    
    # Verify
    print("\n[6/6] Verifying written data...")
    verify_df = spark.read.format("delta").load(output_path)
    print(f"   ✓ Verified {verify_df.count()} records in Delta format")
    
    # Summary statistics
    print("\n" + "="*70)
    print("ETL SUMMARY")
    print("="*70)
    print(f"Input records:        {bronze_df.count()}")
    print(f"Output records:       {verify_df.count()}")
    print(f"Transformations:      5 applied")
    print(f"Output format:        Delta Lake")
    print(f"Output location:      {output_path}")
    
    # Schema evolution
    print("\nOutput Schema:")
    verify_df.printSchema()
    
    spark.stop()
    print("\n✓ Local ETL test completed successfully!")
    print("="*70 + "\n")


if __name__ == "__main__":
    run_local_etl()