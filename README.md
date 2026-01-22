# Apple Retail Data Pipeline

A comprehensive ETL (Extract-Transform-Load) data pipeline built with Apache Spark, simulating an end-to-end analytics solution for Apple retail operations. This project ingests dirty, real-world-like transactional data, applies rigorous data quality checks, and produces analytics-ready datasets that power business intelligence across sales performance, product lifecycle, store benchmarking, and warranty risk analysis.

**Scale**: ~1M+ sales transactions | 75+ global stores | 90 products | 30K+ warranty claims  
**Domain**: Consumer Electronics Retail  
**Analytical Goals**: Revenue optimization, regional store performance, product lifecycle analysis, warranty risk modeling, and demand trend forecasting

---

## Table of Contents

- [Architecture and Data Flow](#architecture-and-data-flow)
- [Data Modeling](#data-modeling)
- [Feature Engineering and Metrics](#feature-engineering-and-metrics)
- [Analytical Insights](#analytical-insights)
- [Performance and Scalability](#performance-and-scalability)
- [Data Quality and Edge Case Handling](#data-quality-and-edge-case-handling)
- [How to Run](#how-to-run)
- [Project Structure](#project-structure)

---

## Architecture and Data Flow

This pipeline follows the **Medallion Architecture** pattern (Bronze → Silver → Gold), enabling incremental data refinement and clear separation between raw ingestion, cleansing, and analytics-ready datasets.

```
┌─────────────────┐
│   RAW (CSV)     │
│                 │
│ Dirty source    │
│ files with      │
│ inconsistencies │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│     BRONZE      │
│   (Parquet)     │
│                 │
│ Schema-on-read  │
│ ingestion with  │
│ type inference  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│     SILVER      │
│   (Cleaned)     │
│                 │
│ Deduplicated,   │
│ standardized,   │
│ FK-validated    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│      GOLD       │
│  (Enriched)     │
│                 │
│ Feature-engineered,
│ analytics-ready │
│ derived metrics │
└─────────────────┘


```

### Layer Responsibilities

| Layer | Purpose | Key Operations |
|-------|---------|----------------|
| **Raw** | Source CSVs with intentional data quality issues | Ingestion point |
| **Bronze** | Raw data persisted as Parquet for schema evolution | `inferSchema`, columnar storage |
| **Silver** | Cleansed, deduplicated, referentially-intact data | Type casting, null handling, FK validation |
| **Gold** | Business-ready datasets with derived metrics | Feature engineering, aggregations, window functions |

### Technology Stack

| Technology | Purpose |
|------------|---------|
| **Apache Spark 4.1** | Distributed data processing engine |
| **PySpark** | Python API for Spark transformations |
| **Parquet** | Columnar storage format for efficient analytics |
| **Jupyter Notebooks** | Interactive development and documentation |
| **Python 3.10** | Runtime environment |

**Why Spark?** The dataset simulates production-scale retail data (~1M transactions). Spark's lazy evaluation, catalyst optimizer, and native Parquet support make it ideal for iterative transformations and large-scale aggregations.

---

## Data Modeling

The pipeline implements a **star schema** design optimized for analytical queries, with a central fact table (Sales) surrounded by dimension tables.

### Entity Relationship Diagram

```
                    ┌──────────────┐
                    │   Category   │
                    │   (Dim)      │
                    └──────┬───────┘
                           │ 1:N
                           │
┌──────────────┐    ┌──────┴───────┐    ┌──────────────┐
│    Store     │    │   Product    │    │   Warranty   │
│    (Dim)     │    │   (Dim)      │    │   (Fact)     │
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │ 1:N               │ 1:N               │ N:1
       │                   │                   │
       └───────────┬───────┘                   │
                   │                           │
            ┌──────┴───────┐                   │
            │    Sales     │───────────────────┘
            │   (Fact)     │
            └──────────────┘
```

### Table Specifications

| Table | Type | Grain | Primary Key | Foreign Keys |
|-------|------|-------|-------------|--------------|
| **sales** | Fact | One row per sale transaction | `sale_id` | `store_id`, `product_id` |
| **warranty** | Fact | One row per warranty claim | `claim_id` | `sale_id` |
| **products** | Dimension | One row per product | `product_id` | `category_id` |
| **stores** | Dimension | One row per store location | `store_id` | — |
| **category** | Dimension | One row per product category | `category_id` | — |

### Key Design Decisions

- **Grain**: Sales fact table is at the transaction level, enabling both aggregate and line-item analysis
- **Degenerate Dimensions**: Transaction identifiers (`sale_id`) stored directly in fact tables
- **Slowly Changing Dimensions**: Not implemented (snapshot approach) — suitable for interview discussion
- **Referential Integrity**: Enforced during Silver layer processing via left-anti joins

---

## Feature Engineering and Metrics

The Gold layer enriches base tables with derived metrics that directly support business questions.

### Sales Metrics

| Metric | Derivation | Analytical Intent |
|--------|------------|-------------------|
| `total_revenue` | `quantity × price` | Core financial KPI for profitability analysis |
| `txn_type` | `CASE WHEN quantity > 0 THEN 'sale' ELSE 'return'` | Distinguishes sales from returns for net revenue |
| `rev_bucket` | Revenue thresholds: high (>$3000), medium (>$750), low (>$0), negative | Segmentation for revenue distribution analysis |
| `rolling_7_day_rev` | Window: 7-day trailing sum partitioned by sale_id | Trend smoothing to identify demand patterns |

### Product Metrics

| Metric | Derivation | Analytical Intent |
|--------|------------|-------------------|
| `price_bucket` | premium (>$1500), midrange (>$500), budget | Product positioning analysis |
| `product_age_yrs` | `YEAR(current_date) - YEAR(launch_date)` | Lifecycle performance correlation |
| `missing_launch_date_flag` | Binary indicator for null launch dates | Data quality tracking |

### Store Metrics

| Metric | Derivation | Analytical Intent |
|--------|------------|-------------------|
| `store_region` | Country-to-region mapping (APAC, NA, EU, ME, SA) | Geographic aggregation for regional analysis |
| `total_store_revenue` | Aggregated sales per store | Store performance ranking |
| `rank_by_region` | `RANK() OVER (PARTITION BY region ORDER BY revenue DESC)` | Identify top/bottom performers within regions |

### Warranty Metrics

| Metric | Derivation | Analytical Intent |
|--------|------------|-------------------|
| `days_to_claim` | `claim_date - sale_date` | Measures time-to-failure for reliability analysis |
| `is_early_failure` | `1 if days_to_claim < 90 else 0` | Flags potential manufacturing defects |
| `claim_%_per_product` | Claims / Units Sold × 100 | Normalizes warranty risk by sales volume |

---

## Analytical Insights

The analysis notebook addresses five key business questions with actionable findings.

### Q1: Revenue by Product Category

**Finding**: Smartphones generate the highest total revenue ($1.08B), but Tablets have the highest average transaction value ($11,798). Accessories show the lowest revenue volatility, indicating a stable, predictable revenue stream.

**Business Implication**: Accessories are undervalued as a revenue stabilizer. Consider bundling strategies to increase attach rates.

### Q2: Store Performance by Region (APAC Focus)

**Finding**: Top APAC stores (Beijing, Macau, Bangkok) outperform the regional average by $10-12M. However, significant variance exists even within the same city (e.g., Melbourne has both top-15 and bottom-half stores).

**Business Implication**: Regional averages mask store-level performance issues. High performers should be studied for operational best practices; underperformers need targeted intervention.

### Q3: Product Age vs. Revenue

**Finding**: Contrary to expectations, older products (5-6 years) generate more revenue and volume than newer launches. This suggests strong brand loyalty and long product lifecycles.

**Business Implication**: Aggressive product replacement cycles may not be necessary. Focus on incremental upgrades rather than full-line refreshes.

### Q4: Warranty Risk Analysis

**Finding**: Early failure rates are extremely low (~0.02%), with average days-to-claim around 700 days (nearly 2 years). This indicates issues are wear-related rather than manufacturing defects.

**Business Implication**: Warranty reserves are likely adequate. Consider predictive maintenance programs to reduce claim volume proactively.

### Q5: Revenue Momentum and Demand Spikes

**Finding**: Daily revenue shows high volatility (likely promotion-driven). Rolling 7-day averages reveal underlying growth trends suitable for forecasting.

**Business Implication**: Time-series forecasting models should use smoothed data. Promotion impact analysis could optimize marketing spend.

---

## Performance and Scalability

### Spark Optimizations Applied

| Optimization | Implementation | Impact |
|--------------|----------------|--------|
| **Columnar Storage** | Parquet format at all layers | Reduced I/O via column pruning |
| **Lazy Evaluation** | Chained transformations without intermediate writes | Catalyst optimizer generates optimal execution plan |
| **Broadcast Joins** | Small dimension tables (category, stores) | Implicit; avoids shuffle for FK validation |
| **Window Functions** | Partitioned by relevant keys | Efficient ranking and rolling aggregations |
| **Predicate Pushdown** | Parquet + filter operations | Data skipping at storage layer |

### Scaling Considerations

For production deployment with larger datasets:

1. **Partitioning**: Partition sales data by `sale_date` (year/month) for time-based query optimization
2. **Bucketing**: Bucket by `store_id` or `product_id` for join optimization
3. **Caching**: Cache frequently-joined dimension tables (`products_df.cache()`)
4. **Cluster Sizing**: Current `local[*]` mode uses all available cores; production would use YARN/Kubernetes
5. **Delta Lake**: Consider migrating to Delta for ACID transactions, schema evolution, and time travel

---

## Data Quality and Edge Case Handling

### Data Quality Issues Detected and Resolved

| Issue | Detection Method | Resolution |
|-------|------------------|------------|
| **Duplicate Rows** | `df.count() - df.distinct().count()` | `dropDuplicates(subset_cols)` |
| **Duplicate Primary Keys** | Count vs. distinct PK count | First occurrence retained |
| **Null Primary Keys** | `col.isNull()` filter | Rows dropped |
| **Invalid Foreign Keys** | Left-anti join with parent table | Orphaned records dropped |
| **Dirty String Values** | Mixed case, whitespace, currency symbols | `lower()`, `trim()`, `regexp_replace()` |
| **Date Format Inconsistency** | Multiple formats (YYYY-MM-DD, DD-MM-YYYY) | `coalesce(try_to_date())` with fallback |

### Null Handling Strategy

| Column Type | Strategy | Rationale |
|-------------|----------|-----------|
| **Primary Keys** | Drop row | Cannot maintain referential integrity |
| **Foreign Keys** | Drop row (for critical FKs) | Prevents orphaned records |
| **Date Fields** | Flag + retain | `missing_*_flag` columns for downstream filtering |
| **Numeric Fields** | Fill with median or 0 | Preserves row for partial analysis |
| **String Fields** | Fill with 'unknown' | Prevents null propagation in joins |

### Profiling Framework

The `validation.py` module provides reusable data quality functions:

```python
profile_table(df, table_name, pk_col, cfk_cols, parent_dfs, pfk_cols)
# Returns: null counts, duplicate rows, duplicate PKs, invalid FKs
```

---

## How to Run

### Prerequisites

- Python 3.10+
- Java 8 or 11 (required by Spark)
- Apache Spark 4.x (or PySpark installed via pip)
- plotly.express
- nbformat 5.X

### Environment Setup

```bash
# Clone the repository
git clone https://github.com/musaab-exe/apple-retail-data-pipeline.git
cd AppleRetailDataPipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install pyspark jupyter
```

### Execution Order

Run notebooks in sequence:

```bash
cd notebooks
jupyter notebook
```

1. **`01_extract.ipynb`** — Ingests raw CSVs into Bronze layer (Parquet)
2. **`02_transformation.ipynb`** — Cleans and validates data, outputs to Silver
3. **`03_load.ipynb`** — Feature engineering, outputs to Gold
4. **`04_analysis.ipynb`** — Business analysis queries and insights

### Configuration Notes

- The `utils/spark_session.py` module handles Hadoop configuration for Windows compatibility
- Spark runs in local mode (`local[*]`) using all available CPU cores
- No external dependencies beyond PySpark are required

---

## Project Structure

```
AppleRetailDataPipeline/
│
├── data/
│   ├── raw/                    # Source CSV files (dirty data)
│   │   ├── category.csv
│   │   ├── products.csv
│   │   ├── sales.csv           # ~1M transactions
│   │   ├── stores.csv
│   │   └── warranty.csv
│   ├── bronze_ingested/        # Raw data as Parquet
│   ├── silver_cleaned/         # Cleaned, validated data
│   └── gold/                   # Analytics-ready datasets
│
├── notebooks/
│   ├── 01_extract.ipynb        # Bronze layer ingestion
│   ├── 02_transformation.ipynb # Silver layer cleaning
│   ├── 03_load.ipynb           # Gold layer enrichment
│   └── 04_analysis.ipynb       # Business analytics
│
├── utils/
│   ├── spark_session.py        # Spark configuration
│   ├── transformation.py       # Reusable transformation functions
│   ├── validation.py           # Data quality profiling
│   └── hadoop/                 # Windows Hadoop binaries
│
└── README.md
```

---

## Author
**Musaab Mohammed**

Email: musaabjawed@gmail.com  

LinkedIn: [linkedin.com/in/musaab-jawed-mohammed](https://linkedin.com/in/musaab-jawed-mohammed-99415228b) 

GitHub: [github.com/musaab-exe](https://github.com/musaab-exe)

Built as a portfolio project demonstrating end-to-end data engineering capabilities including:

- Distributed data processing with Apache Spark
- Data quality engineering and validation frameworks
- Dimensional modeling and star schema design
- Feature engineering for business analytics
- Production-ready code organization and documentation

---

*This project uses simulated data inspired by Apple's retail ecosystem. All data is synthetic and for demonstration purposes only.*
