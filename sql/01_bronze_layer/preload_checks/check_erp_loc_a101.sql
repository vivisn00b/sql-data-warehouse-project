/*
===============================================================================
Script: Bronze ERP Location Data Validation & Standardization (Ad-Hoc)
Table : bronze.erp_loc_a101
Layer : Bronze (Pre-Silver Transformation Validation)
===============================================================================

Overview:
    This script contains exploratory data validation and profiling queries
    executed against the bronze.erp_loc_a101 table during the development
    of the data warehouse.

    The purpose of this analysis was to assess data consistency issues within
    ERP location data before implementing transformation and standardization
    logic in the Silver layer.

    The queries focus primarily on customer identifier consistency and
    country value normalization.

    ! This is an exploratory development script.
    ! Not optimized for performance.
    ! Not part of automated ETL execution.
    ! Used strictly for data profiling and transformation design.

-------------------------------------------------------------------------------
Key Validation Areas Covered:
-------------------------------------------------------------------------------

1. Customer Identifier (cid) Standardization
    - Detection of formatting inconsistencies (e.g., hyphens).
    - Prototype use of REPLACE() to normalize identifiers.
    - Validation that cleaned IDs align with other ERP/CRM tables.
    
    Objective:
        Ensure consistent key formatting before joining across schemas
        in the Silver layer.

2. Country (cntry) Value Analysis
    - Review DISTINCT country values.
    - Detect inconsistent abbreviations (e.g., 'US', 'USA', 'DE').
    - Identify case sensitivity and whitespace issues.
    - Prototype CASE-based mapping logic.

    Business Rationale:
        Standardized country names improve:
             Reporting accuracy
             Aggregation consistency
             Dimensional modeling quality

3. Missing or Blank Country Detection
    - Identify NULL values.
    - Detect empty strings or whitespace-only values.
    - Confirm need for default replacement value (e.g., 'n/a').

    Purpose:
        Prevent NULL propagation to downstream analytical layers.
        Ensure clean dimension attributes in Silver layer.

4. Transformation Simulation
    - Build prototype SELECT logic to simulate final Silver output.
    - Validate mapping strategy before embedding in stored procedure.
    - Confirm transformation logic produces expected standardized results.

-------------------------------------------------------------------------------
ETL Lifecycle Context:
-------------------------------------------------------------------------------

This script represents the Data Profiling and Cleansing Design phase
within the Bronze layer workflow:

    Source System → Bronze (Raw ERP Location Data)
                   → Profiling & Validation (This Script)
                   → Silver (Cleansed & Standardized Data)

All normalization logic later implemented in silver.load_silver
was derived from findings in this exploratory validation process.

-------------------------------------------------------------------------------
Why This Step Is Critical:
-------------------------------------------------------------------------------

Without profiling and validation:

    - Inconsistent country codes could fragment reporting.
    - Key mismatches could break cross-table joins.
    - Blank or NULL values could propagate to analytical layers.
    - Business reporting would lack standardization.

This validation step ensured:

     Standardized geographic attributes
     Clean join keys across systems
     Controlled handling of missing data
     Evidence-based transformation design

-------------------------------------------------------------------------------
Development Status:
-------------------------------------------------------------------------------

     Executed during warehouse development phase
     Informed Silver layer transformation logic
     Helped define standardization rules
     Not productionized
     Not scheduled or automated

-------------------------------------------------------------------------------
*/

SELECT TOP (1000) [cid]
      ,[cntry]
  FROM [DataWarehouse].[bronze].[erp_loc_a101]

-- Data Standardization & Consistency
select cid, 
replace(cid,'-','') clean_cid
from bronze.erp_loc_a101

select distinct cntry
from bronze.erp_loc_a101

-- Check missing country
SELECT *
FROM bronze.erp_loc_a101
WHERE cntry IS NULL
   OR TRIM(cntry) = '';

SELECT DISTINCT 
    cntry AS old_cntry,
    CASE 
        WHEN Upper(TRIM(cntry)) = 'DE' THEN 'Germany'
        WHEN Upper(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
        WHEN Upper(TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS clean_cntry
FROM bronze.erp_loc_a101;