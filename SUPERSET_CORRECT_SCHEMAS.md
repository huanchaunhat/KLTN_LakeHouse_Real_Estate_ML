### 🏛️ **gold.dim_legal_status** (4 columns)

```sql
- legal_status_id         int          -- Primary key
- legal_status            string
- legal_status_category   string
- description             string
```

### 📍 **dim_locations** (6 columns)

```sql
- location_id             int          -- Primary key
- full_address            string
- province                string
- district                string
- ward                    void
- region                  string
```

### 🏠 **dim_properties** (14 columns)

```sql
- property_id             string       -- Primary key
- title                   string
- area                    double
- frontage                double
- floors                  int
- bedrooms                int
- bathrooms               int
- legal_status            string
- house_direction         string
- created_at              timestamp
- updated_at_ts           timestamp
- valid_from              timestamp
- valid_to                void
- is_current              boolean
```

### 📈 **gold.fct_properties** (15 columns) Main Fact Table

```sql
- property_id              string       -- PK, FK to dim_properties
- location_id              int          -- FK to dim_locations
- legal_status_id          int          -- FK to dim_legal_status
- date_key                 date         -- FK to dim_date.date_day
- price_in_billions        double       -- Price in billions VNĐ (NOT NULL)
- area                     double       -- Area in m² (NOT NULL)
- price_per_m2_millions    double       -- Calculated: (price*1000)/area
- floors                   int          -- nullable
- bedrooms                 int          -- nullable
- bathrooms                int          -- nullable
- house_direction          string       -- nullable
- title                    string       -- Property title (NOT NULL)
- images                   array<string>-- Image URLs
- updated_at_ts            timestamp    -- Last updated
- created_at               timestamp    -- First seen
```

### 📊 **gold.fct_daily_summary** (13 columns)

```sql
- report_date                         date    -- Aggregation date
- total_new_listings                  bigint  -- Count of properties
- total_value_listed_billions         double  -- Sum of all prices
- avg_price_per_m2_millions           double  -- Average price per m²
- min_price_per_m2_millions           double  -- Minimum price per m²
- max_price_per_m2_millions           double  -- Maximum price per m²
- avg_area                            double  -- Average property area
- avg_bedrooms                        double  -- Avg bedrooms (ignores NULL)
- avg_bathrooms                       double  -- Avg bathrooms (ignores NULL)
- avg_floors                          double  -- Avg floors (ignores NULL)
- properties_with_bedroom_info        bigint  -- Count non-NULL bedrooms
- properties_with_bathroom_info       bigint  -- Count non-NULL bathrooms
- properties_with_floor_info          bigint  -- Count non-NULL floors
```

### 🔍 **gold.fct_data_quality_report** (4 columns)

```sql
- report_date             timestamp
- data_quality_flag       string
- record_count            bigint
- percentage              decimal(38,14)
```
