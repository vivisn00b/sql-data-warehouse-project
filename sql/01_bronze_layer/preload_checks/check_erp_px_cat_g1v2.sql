/*
===============================================================================
Script: Bronze ERP Product Category Data Validation & Cleansing (Ad-Hoc)
Table : bronze.erp_px_cat_g1v2
Layer : Bronze (Pre-Silver Transformation Validation)
===============================================================================

Overview:
    This script contains exploratory queries executed against the 
    bronze.erp_px_cat_g1v2 table to validate and profile product category
    and maintenance data before designing Silver layer transformations.

    The purpose of this analysis is to detect inconsistencies, missing 
    values, and formatting issues, as well as to prototype standardization 
    logic for the Silver layer.

    ! This is an investigative script.
    ! Not optimized for performance.
    ! Not intended for automated ETL execution.
    ! Strictly used for data profiling and transformation design.

-------------------------------------------------------------------------------
Key Validation Areas Covered:
-------------------------------------------------------------------------------

1. Null & Empty Value Detection
    - Identify NULLs in id, cat, subcat, maintenance.
    - Identify empty strings in any of the key columns.
    - Objective: Prevent propagation of missing data into the Silver layer.

2. Whitespace & Formatting Checks
    - Detect leading/trailing spaces in id, cat, subcat, maintenance.
    - Confirm need for TRIM() function in Silver transformations.
    - Purpose: Ensure consistent key formatting for downstream joins.

3. Data Standardization & Consistency
    - Normalize 'maintenance' column to 'Yes'/'No'.
    - Detect invalid values outside the accepted set ('Yes', 'No').
    - Objective: Standardize categorical attributes for reporting.

4. Referential Integrity Checks
    - Compare ERP product IDs against CRM product keys.
    - Identify mismatches between id and category (cat) values.
    - Purpose: Ensure Silver layer data aligns with related tables.

5. Duplicate Detection
    - Identify repeated id values.
    - Helps determine deduplication strategy during Silver transformations.

6. Transformation Simulation
    - Prototype SELECT statements for cleaned and normalized output.
    - Validate trimming, COALESCE, and CASE-based standardization logic.
    - Ensure business rules are consistent before embedding in stored procedures.

-------------------------------------------------------------------------------
ETL Lifecycle Context:
-------------------------------------------------------------------------------

This script represents the Data Profiling and Cleansing Design phase
for the Bronze layer:

    Source System → Bronze (Raw ERP Product Category Data)
                   → Profiling & Validation (This Script)
                   → Silver (Cleansed & Standardized Data)

All normalization and validation logic later implemented in
silver.load_silver was derived from findings in this exploratory analysis.

-------------------------------------------------------------------------------
Why This Step Is Critical:
-------------------------------------------------------------------------------

Without proper profiling:

    - Invalid maintenance values could distort analytics.
    - Formatting inconsistencies could break joins or lookups.
    - NULL or empty values could propagate into Silver layer metrics.
    - Duplicate IDs could cause aggregation errors.

This validation step ensures:

     Clean, trimmed, and standardized categorical attributes
     Correct handling of missing or invalid values
     Consistency between Bronze ERP and CRM data
     Evidence-based transformation rules for Silver layer

-------------------------------------------------------------------------------
Development Status:
-------------------------------------------------------------------------------

     Executed during warehouse development
     Informed Silver layer transformation logic
     Helped define data normalization rules
     Not productionized
     Not part of automated ETL pipeline

-------------------------------------------------------------------------------
*/

SELECT TOP (10) [id]
      ,[cat]
      ,[subcat]
      ,[maintenance]
  FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2]

-- Check for NULLs
SELECT *
FROM DataWarehouse.bronze.erp_px_cat_g1v2
WHERE id IS NULL
   OR cat IS NULL
   OR subcat IS NULL
   OR maintenance IS NULL;

-- Check for empty string
SELECT *
FROM DataWarehouse.bronze.erp_px_cat_g1v2
WHERE trim(id) =''
   OR TRIM(cat) = ''
   OR TRIM(subcat) = ''
   OR TRIM(maintenance) = ''

-- Check for Unwanted Spaces
select distinct cat
from bronze.erp_px_cat_g1v2

select *
from bronze.erp_px_cat_g1v2
where cat != TRIM(cat)
or subcat != TRIM(subcat)
or maintenance!=TRIM(maintenance)

-- Data Standardization & Consistency
select distinct maintenance
from bronze.erp_px_cat_g1v2

-- Check Invalid Maintenance Values
SELECT DISTINCT maintenance
FROM DataWarehouse.bronze.erp_px_cat_g1v2
WHERE maintenance NOT IN ('Yes','No');

-- Check ID vs Category Mismatch
SELECT *
FROM DataWarehouse.bronze.erp_px_cat_g1v2
WHERE id not in (
select REPLACE(SUBSTRING(prd_key, 1,5), '-','_') cat_id
from bronze.crm_prd_info)

-- Check for Duplicates
SELECT id, COUNT(*)
FROM DataWarehouse.bronze.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1;

SELECT
    TRIM(id) AS id,
    TRIM(cat) AS cat,
    TRIM(subcat) AS subcat,
    CASE 
        WHEN UPPER(TRIM(maintenance)) = 'YES' THEN 'Yes'
        WHEN UPPER(TRIM(maintenance)) = 'NO'  THEN 'No'
        ELSE 'No'
    END AS maintenance
FROM bronze.erp_px_cat_g1v2
WHERE
    -- Remove NULL and empty strings
    NULLIF(TRIM(id), '') IS NOT NULL
    AND NULLIF(TRIM(cat), '') IS NOT NULL
    AND NULLIF(TRIM(subcat), '') IS NOT NULL
    AND NULLIF(TRIM(maintenance), '') IS NOT NULL
    -- Allow only valid maintenance values
    AND UPPER(TRIM(maintenance)) IN ('YES', 'NO');

SELECT
    -- ID (should ideally never be unknown, but included for completeness)
    COALESCE(NULLIF(TRIM(id), ''), 'Unknown') AS id,

    -- Category
    COALESCE(NULLIF(TRIM(cat), ''), 'Unknown') AS cat,

    -- Subcategory
    COALESCE(NULLIF(TRIM(subcat), ''), 'Unknown') AS subcat,

    -- Maintenance normalization
    CASE 
        WHEN UPPER(TRIM(maintenance)) = 'YES' THEN 'Yes'
        WHEN UPPER(TRIM(maintenance)) = 'NO'  THEN 'No'
        ELSE 'No'
    END AS maintenance
FROM bronze.erp_px_cat_g1v2;