/*
===============================================================================
Script: Bronze Layer Data Profiling, Validation & Cleansing (Exploratory Script)
Layer : Bronze (Pre-Transformation Analysis)
===============================================================================

Overview:
    This script contains exploratory SQL queries executed against Bronze layer
    tables during the data warehouse development phase.

    It was used to analyze raw source data, uncover data quality issues,
    validate structural assumptions, and design transformation logic that was
    later implemented in the Silver layer stored procedures.

    The script reflects iterative data investigation and is intentionally
    exploratory in nature.

    ! Not production code
    ! Not optimized for performance
    ! Not part of automated ETL execution
    ! Used strictly for development and validation purposes

-------------------------------------------------------------------------------
Primary Goals:
-------------------------------------------------------------------------------

1. Structural Analysis
    - Inspect raw dataset structure and column behavior.
    - Validate expected data types and patterns.
    - Identify anomalies in key fields.

2. Data Quality Assessment
    - Detect NULL values in business-critical columns.
    - Identify duplicate records in assumed primary keys.
    - Evaluate integrity of natural keys before transformation.

3. Duplicate Resolution Design
    - Apply ROW_NUMBER() window function for deduplication analysis.
    - Partition by business key to identify record versions.
    - Order by timestamp or relevant date column to determine:
        • Most recent record
        • Historical record
        • Records to retain in Silver layer

4. Data Cleansing Discovery
    - Detect leading and trailing whitespace.
    - Identify inconsistent capitalization.
    - Reveal formatting inconsistencies across categorical values.
    - Validate need for TRIM(), UPPER(), and normalization rules.

5. Standardization & Mapping Validation
    - Examine DISTINCT categorical values.
    - Detect inconsistent coding conventions.
    - Design CASE-based transformation mappings.
    - Ensure business-friendly standardized outputs.

6. Transformation Prototyping
    - Simulate final Silver-layer SELECT logic.
    - Validate cleansing logic before embedding into stored procedures.
    - Confirm correctness of deduplication and mapping strategies.

-------------------------------------------------------------------------------
ETL Methodology Context:
-------------------------------------------------------------------------------

This script represents the "Data Understanding" phase of the ETL lifecycle:

    Source → Bronze (Raw) → Profiling → Silver (Cleansed) → Gold (Analytics)

Key Principle:
    Transformation logic must be evidence-based.
    All cleansing rules implemented in Silver were derived
    from findings in this exploratory analysis.

Why This Matters:
    - Prevents incorrect business rule assumptions.
    - Avoids accidental data loss during deduplication.
    - Reduces propagation of dirty data to downstream layers.
    - Improves long-term warehouse reliability.

-------------------------------------------------------------------------------
Development & Governance Notes:
-------------------------------------------------------------------------------

    Executed during warehouse build phase
    Informed Silver layer transformation design
    Helped define data quality standards
    Supports documentation of cleansing decisions
    Not scheduled
    Not parameterized
    Not part of production pipeline

-------------------------------------------------------------------------------
*/

SELECT TOP (1000) [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[bronze].[crm_sales_details]

-- Check for Invalid Dates
select *
from bronze.crm_sales_details
where sls_order_dt is null or sls_due_dt is null or sls_ship_dt is null
or len(sls_order_dt) != 8 or LEN(sls_due_dt) != 8 or LEN(sls_ship_dt) != 8
OR sls_due_dt > 20300101     -- max order date set by business
OR sls_due_dt < 19990101     -- suppose business started on 01/01/1999

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
select *
from bronze.crm_sales_details
where sls_order_dt>sls_ship_dt
or sls_order_dt>sls_due_dt

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
select *
from bronze.crm_sales_details
where sls_sales != sls_quantity*sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT 
    sls_sales as old_sales,
    sls_quantity as old_qty,
    sls_price as old_price,
    case when sls_sales is null or sls_sales<=0 or sls_sales!=abs(sls_quantity)*ABS(sls_price)
    then abs(sls_quantity) * ABS(sls_price)
    else sls_sales
    end as sls_sales,
    case when sls_quantity is null or sls_quantity<=0
    then sls_sales/nullif(abs(sls_price),0)
    else sls_quantity
    end as sls_quantity,
    case when sls_price is null or sls_price<=0
    then sls_sales/nullif(sls_quantity, 0)      -- futureproofing: if we get any 0 value in future, to prevent from giving error
    else sls_price
    end as sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


-- If Price + Qty are source of truth
SELECT 
    sls_sales as old_sales,
    sls_quantity as old_qty,
    sls_price as old_price,

    -- Clean quantity
    case 
        when sls_quantity is null or sls_quantity <= 0 
        then null
        else sls_quantity
    end as sls_quantity,

    -- Clean price
    case 
        when sls_price is null or sls_price <= 0 
        then null
        else abs(sls_price)
    end as sls_price,

    -- Always derive sales from cleaned qty * price
    case 
        when sls_quantity > 0 and sls_price > 0
        then sls_quantity * abs(sls_price)
        else null
    end as sls_sales

FROM bronze.crm_sales_details

-- According to general business logic
SELECT 
    sls_sales AS old_sales,
    sls_quantity AS old_qty,
    sls_price AS old_price,

    -- Quantity is the source of truth: just take absolute value
    abs(sls_quantity) AS sls_quantity,

    -- Recalculate sales always from quantity * price
    CASE 
        WHEN sls_quantity IS NULL OR sls_price IS NULL OR sls_quantity <= 0 OR sls_price <= 0
        THEN NULL  -- cannot calculate sales if qty or price invalid
        ELSE abs(sls_quantity) * abs(sls_price)
    END AS sls_sales,

    -- Correct price only if NULL or <= 0, otherwise keep existing
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN CASE 
                WHEN sls_quantity IS NULL OR sls_quantity = 0
                THEN NULL  -- prevent divide by zero
                ELSE abs(sls_sales) / abs(sls_quantity)
             END
        ELSE sls_price
    END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;