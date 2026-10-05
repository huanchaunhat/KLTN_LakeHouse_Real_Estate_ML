# 🌟 STAR SCHEMA DESIGN - GOLD LAYER

## 📊 Tổng Quan

Gold layer được thiết kế theo **Star Schema** - một design pattern tối ưu cho Data Warehouse, cho phép:

- Query performance cao với ít JOIN operations
- Business logic rõ ràng và dễ hiểu
- Scalability tốt khi data tăng trưởng
- BI tools integration dễ dàng

---

## 🏗️ STAR SCHEMA DIAGRAM

```
       ┌─────────────────┐                        ┌──────────────────┐
       │ dim_locations   │                        │ dim_legal_status │
       ├─────────────────┤                        ├──────────────────┤
       │ location_id(PK) │                        │ legal_status_id  │
       │ full_address    │                        │ legal_status     │
       │ province        │                        │ category         │
       │ district        │                        │ description      │
       │ ward (NULL)     │                        └────────┬─────────┘
       │ region          │                                 │
       └────────┬────────┘                                 │
                │                                          │
                │                                          │
                └──────────────────┬───────────────────────┘
                                   │
                          ┌────────▼────────────┐
                          │  fct_properties     │ ◄─── CENTRAL FACT TABLE
                          ├─────────────────────┤
                          │ property_id (PK)    │
                          │ location_id (FK)    │──────┐
                          │ legal_status_id(FK) │──────┤
                          │ date_key (DATE)     │      │
                          │ ─────────────────── │      │
                          │ price_in_billions   │◄─ MEASURES (15 columns)
                          │ area                │
                          │ price_per_m2_mil    │
                          │ floors (nullable)   │
                          │ bedrooms (nullable) │
                          │ bathrooms (nullable)│
                          │ house_direction     │
                          │ title               │
                          │ images (array)      │
                          │ updated_at_ts       │
                          │ created_at          │
                          └─────────────────────┘
                                   │
                                   │ property_id (FK)
                                   │
                          ┌────────▼─────────────┐
                          │  dim_properties      │ ◄─── SCD Type 2
                          ├──────────────────────┤
                          │ property_id (PK)     │
                          │ title                │
                          │ area                 │
                          │ frontage             │
                          │ floors               │
                          │ bedrooms             │
                          │ bathrooms            │
                          │ legal_status         │
                          │ house_direction      │
                          │ created_at           │
                          │ updated_at_ts        │
                          │ valid_from           │
                          │ valid_to (NULL)      │
                          │ is_current (boolean) │
                          └──────────────────────┘


       ┌────────────────────────┐              ┌──────────────────────────┐
       │ fct_daily_summary      │              │ fct_data_quality_report  │
       ├────────────────────────┤              ├──────────────────────────┤
       │ report_date            │              │ report_date              │
       │ total_new_listings     │              │ data_quality_flag        │
       │ total_value_billions   │              │ record_count             │
       │ avg_price_per_m2       │              │ percentage               │
       │ min/max_price_per_m2   │              └──────────────────────────┘
       │ avg_area/bedrooms/...  │
       │ properties_with_info   │              ◄─── AGGREGATE FACTS
       └────────────────────────┘              (Pre-computed metrics)
```

---

## 📁 CẤU TRÚC TABLES

### **Dimension Tables (3):**

#### 1. `dim_locations` (6 columns)

- **Purpose**: Địa chỉ với geographic hierarchy
- **Columns**:
  - `location_id` (INT, PK) - Hash-based stable surrogate key
  - `full_address` (STRING) - Complete address
  - `province` (STRING) - Extracted province
  - `district` (STRING) - Extracted district
  - `ward` (NULL) - Placeholder (always NULL)
  - `region` (STRING) - Miền Nam/Bắc/Trung
- **Key**: location_id (surrogate key, hash-based for stability)
- **Hierarchy**: Region → Province → District
- **File**: `dbt/models/marts/dim_locations.sql`

#### 2. `dim_legal_status` (4 columns)

- **Purpose**: Lookup table cho tình trạng pháp lý
- **Columns**:
  - `legal_status_id` (INT, PK) - Hash-based surrogate key
  - `legal_status` (STRING) - Original status text
  - `legal_status_category` (STRING) - Categorized (Full/Partial/Unknown)
  - `description` (STRING) - Human-readable explanation
- **Key**: legal_status_id (surrogate key, hash-based)
- **Type**: Slowly changing (incremental refresh)
- **File**: `dbt/models/marts/dim_legal_status.sql`

#### 3. `dim_properties` (14 columns) - SCD Type 2

- **Purpose**: Property attributes with historical tracking
- **Columns**:
  - `property_id` (STRING, PK) - Business key
  - `title`, `area`, `frontage`, `floors`, `bedrooms`, `bathrooms`
  - `legal_status`, `house_direction`
  - `created_at`, `updated_at_ts`
  - `valid_from`, `valid_to`, `is_current` (SCD Type 2 fields)
- **Key**: property_id (natural/business key)
- **SCD**: Type 2 - Track changes over time (valid_from/to, is_current flag)
- **File**: `dbt/models/marts/dim_properties.sql`

### **Fact Tables (3):**

#### 1. `fct_properties` (15 columns) - Transaction Fact ⭐

- **Purpose**: Central fact table - daily property listings (star schema center)
- **Grain**: One row per property listing
- **Foreign Keys**:
  - `location_id` (INT) → dim_locations.location_id
  - `legal_status_id` (INT) → dim_legal_status.legal_status_id
  - `property_id` (STRING) → dim_properties.property_id
- **Date Field**:
  - `date_key` (DATE) - Derived from updated_at_ts (YYYY-MM-DD format)
- **Measures** (Additive & Semi-additive):
  - `price_in_billions` (DOUBLE) - Listing price in billions VNĐ
  - `area` (DOUBLE) - Property area in m²
  - `price_per_m2_millions` (DOUBLE) - Calculated: (price\*1000)/area
  - `floors`, `bedrooms`, `bathrooms` (INT, nullable) - Property features
  - `house_direction` (STRING, nullable) - Orientation
- **Degenerate Dimensions**:
  - `title` (STRING) - Property title
  - `images` (ARRAY<STRING>) - Image URLs
- **Timestamps**:
  - `updated_at_ts`, `created_at` (TIMESTAMP)
- **Data Quality**: Only records with data_quality_flag='VALID' (filters out NULL prices/addresses)
- **File**: `dbt/models/marts/fct_properties.sql`

#### 2. `fct_daily_summary` (13 columns) - Aggregate Fact

- **Purpose**: Pre-aggregated daily metrics for performance
- **Grain**: One row per day (report_date)
- **Measures**:
  - `total_new_listings` (BIGINT) - Count of properties
  - `total_value_listed_billions` (DOUBLE) - Sum of all prices
  - `avg_price_per_m2_millions` (DOUBLE) - Average price/m²
  - `min_price_per_m2_millions`, `max_price_per_m2_millions` (DOUBLE)
  - `avg_area` (DOUBLE) - Average property size
  - `avg_bedrooms`, `avg_bathrooms`, `avg_floors` (DOUBLE) - Avg features (NULL ignored)
  - `properties_with_bedroom_info` (BIGINT) - Count non-NULL bedrooms
  - `properties_with_bathroom_info`, `properties_with_floor_info` (BIGINT)
- **Use Case**: Time-series dashboards, trend analysis
- **File**: `dbt/models/marts/fct_daily_summary.sql`

#### 3. `fct_data_quality_report` (4 columns) - Aggregate Fact

- **Purpose**: Pipeline health monitoring
- **Grain**: One row per (report_date, data_quality_flag)
- **Measures**:
  - `data_quality_flag` (STRING) - VALID/MISSING_PRICE/MISSING_ADDRESS/etc.
  - `record_count` (BIGINT) - Number of records with this flag
  - `percentage` (DOUBLE) - % of total records
- **Use Case**: Monitor data quality trends, alert on issues
- **File**: `dbt/models/marts/fct_data_quality_report.sql`

---

## 🚀 DEPLOYMENT

### 1. Build models:

```bash
dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select marts
```

### 2. Run tests:

```bash
dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select marts
```
