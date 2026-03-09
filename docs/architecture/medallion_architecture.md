# Azure Synapse Medallion Architecture

## Overview
This project implements a production-grade Lakehouse architecture using Azure Synapse Analytics with the Medallion pattern (Bronze-Silver-Gold).

## Architecture Layers

### Bronze Layer (Raw Data)
- **Purpose**: Immutable storage of raw data from source systems
- **Format**: Parquet/Delta Lake
- **Retention**: 90 days with time travel
- **Optimization**: None (keep raw)
- **Access**: Restricted to data engineers only

### Silver Layer (Cleansed)
- **Purpose**: Cleaned, deduplicated, and validated data
- **Format**: Delta Lake with Z-Ordering
- **Transformations**: 
  - Schema enforcement
  - Data quality checks (Great Expectations)
  - Deduplication and SCD handling
  - Business rule validation
- **Retention**: 1 year with versioning
- **Optimization**: Z-Order by query patterns

### Gold Layer (Aggregated)
- **Purpose**: Business-ready aggregates and star schema
- **Format**: Delta Lake + Dedicated SQL Pool external tables
- **Models**: Star schema with slowly changing dimensions
- **Optimization**: 
  - Hash distribution for large tables
  - Replicated tables for dimensions
  - Materialized views for common aggregates

## Data Flow
