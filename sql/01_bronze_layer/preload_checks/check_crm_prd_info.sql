/*
===============================================================================
Script: Bronze Layer Data Validation & Cleansing Analysis (Ad-Hoc Script)
Layer : Bronze (Pre-Silver Validation)
===============================================================================

Purpose:
    This script contains exploratory queries executed on Bronze layer tables
    prior to implementing transformation logic in the Silver layer.

    It represents the Data Profiling and Quality Assessment phase of the ETL
    lifecycle and was used to investigate structural and business rule issues
    within raw source data.

    The script is intentionally written in an investigative, step-by-step manner
    and may appear unstructured or repetitive. This reflects real-world data
    exploration during development.

    ! This is NOT a production-ready script.
    ! This is NOT intended for scheduled execution.
    ! This was used strictly for validation and transformation design.

-------------------------------------------------------------------------------
Objectives of This Script:
-------------------------------------------------------------------------------

1. Data Profiling
    - Review overall structure and sample records.
    - Understand column distributions and patterns.
    - Validate assumptions about source system behavior.

2. Primary Key & Integrity Validation
    - Detect NULL values in key columns.
    - Identify duplicate records.
    - Validate uniqueness constraints before enforcing business rules.

3. Duplicate Handling Strategy Design
    - Use ROW_NUMBER() with appropriate partitioning.
    - Determine correct ordering logic (e.g., latest record).
    - Define which records should be retained vs excluded.

4. Data Cleansing Observations
    - Identify leading/trailing whitespace issues.
    - Detect inconsistent casing.
    - Highlight formatting inconsistencies.
    - Confirm need for TRIM(), UPPER(), and standardization logic.

5. Data Standardization & Consistency Checks
    - Review distinct values for categorical columns.
    - Identify inconsistent codes or representations.
    - Determine mapping logic required in Silver layer (CASE statements).

6. Transformation Simulation
    - Build prototype SELECT statements simulating final Silver output.
    - Validate cleansing logic before embedding into stored procedures.
    - Ensure business rules produce expected results.

-------------------------------------------------------------------------------
Why This Step Is Critical in ETL:
-------------------------------------------------------------------------------

Data Engineering Best Practice:
    Never implement transformations blindly.
    Always profile and understand raw data first.

Bronze Layer Philosophy:
    - Store raw data exactly as received.
    - Perform investigation and validation separately.
    - Design transformations based on observed issues.

Without proper profiling:
    - Duplicate handling may be incorrect.
    - Business logic may remove valid records.
    - Data quality issues may propagate to downstream layers.

This script documents the analytical groundwork that informed the
design of the Silver layer transformation procedures.

-------------------------------------------------------------------------------
Development Context:
-------------------------------------------------------------------------------

    Executed during initial data warehouse development
    Used to design cleansing and normalization logic
    Helped validate business rules before production deployment
    Not optimized for performance
    Not part of automated ETL pipeline

-------------------------------------------------------------------------------
*/

SELECT TOP (1000) [prd_id]
      ,[prd_key]
      ,[prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_dt]
  FROM [DataWarehouse].[bronze].[crm_prd_info]

-- Check for NULLs or Duplicates in Primary Key
select prd_id, COUNT(*)
from bronze.crm_prd_info
group by prd_id
having COUNT(*) > 1 OR prd_id is null

-- Check for Unwanted Spaces
select prd_nm
from bronze.crm_prd_info
where prd_nm <> TRIM(prd_nm)

-- Check for NULLs or Negative Values in Cost
select prd_id, prd_nm, prd_cost
from bronze.crm_prd_info
where prd_cost<0 or prd_cost is null

-- Data Standardization & Consistency
select distinct prd_line
from bronze.crm_prd_info

select prd_id, prd_nm, prd_cost, prd_line
from bronze.crm_prd_info
where prd_line is null

-- Checking for unordered product
select REPLACE(SUBSTRING(prd_key, 1,5), '-','_') cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) prd_key
from bronze.crm_prd_info
where SUBSTRING(prd_key,7,LEN(prd_key)) not in (
select sls_prd_key
from bronze.crm_sales_details
)

-- Check for Invalid Date Orders (Start Date > End Date)
select *
from bronze.crm_prd_info
where prd_start_dt > prd_end_dt

select prd_id,
REPLACE(SUBSTRING(prd_key, 1,5), '-','_') cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) prd_key,
prd_start_dt, prd_end_dt
from bronze.crm_prd_info

select prd_id,
REPLACE(SUBSTRING(prd_key, 1,5), '-','_') cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) prd_key,
prd_start_dt, prd_end_dt,
ROW_NUMBER() OVER (Partition by prd_key order by prd_start_dt ASC) latest_prd
from bronze.crm_prd_info
where REPLACE(SUBSTRING(prd_key, 1,5), '-','_') = 'AC_HE'

-- OPTION1: Swap prd_start_dt and prd_end_dt for all but the last row per prd_key
WITH cte AS (
    SELECT 
        prd_id,
        prd_key,
        prd_start_dt,
        prd_end_dt,
        ROW_NUMBER() OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC) AS rn,
        COUNT(*) OVER (PARTITION BY prd_key) AS max_rn
    FROM bronze.crm_prd_info
    WHERE REPLACE(SUBSTRING(prd_key, 1,5), '-','_') = 'AC_HE'
)

SELECT
    prd_id,
    prd_key,
    CASE 
        WHEN rn <> max_rn THEN prd_end_dt
        ELSE prd_start_dt
    END AS prd_start_dt,
    CASE 
        WHEN rn <> max_rn THEN prd_start_dt
        ELSE prd_end_dt
    END AS prd_end_dt,
    rn AS latest_prd
FROM cte
ORDER BY prd_key, rn

-- Sort by prd_end_dt in the window function, keeping NULLs first
-- to simplify the swap logic and prevent unintended swapping if a NULL prd_end_dt appears mid-sequence
WITH cte AS (
    SELECT 
        prd_id,
        prd_key,
        prd_start_dt,
        prd_end_dt,
        ROW_NUMBER() OVER (PARTITION BY prd_key ORDER BY prd_end_dt ASC) AS rn
    FROM bronze.crm_prd_info
)

SELECT
    prd_id,
    prd_key,
    CASE 
        WHEN rn != 1 THEN prd_end_dt
        ELSE prd_start_dt
    END AS prd_start_dt,
    CASE 
        WHEN rn !=1 THEN prd_start_dt
        ELSE prd_end_dt
    END AS prd_end_dt
FROM cte

-- Option 2: Swap each row’s end_dt with the next row’s start_dt minus one day
with cte_2 as (
select prd_id,
REPLACE(SUBSTRING(prd_key, 1,5), '-','_') cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) prd_key,
prd_nm,
prd_start_dt, prd_end_dt,
LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) as test_end_dt
from bronze.crm_prd_info
)

select t1.prd_id
      ,t1.prd_key
      ,t1.prd_nm
      ,t1.prd_cost
      ,t1.prd_line
      ,t1.prd_start_dt
      ,t2.test_end_dt AS prd_end_dt
from bronze.crm_prd_info t1
JOIN cte_2 t2 on t1.prd_id = t2.prd_id