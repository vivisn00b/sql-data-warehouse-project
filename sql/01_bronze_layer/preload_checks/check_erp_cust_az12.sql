/*
===============================================================================
Script: Bronze ERP Customer Data Validation & Cleansing Analysis (Ad-Hoc)
Table : bronze.erp_cust_az12
Layer : Bronze (Pre-Silver Transformation Validation)
===============================================================================

Overview:
    This script contains exploratory validation queries executed against the
    bronze.erp_cust_az12 table during the development of the data warehouse.

    The purpose of this analysis was to assess data quality issues within
    ERP customer demographic data before implementing transformation logic
    in the Silver layer.

    The queries were used to investigate anomalies, standardize categorical
    values, and validate cleansing rules prior to embedding them into the
    silver.load_silver stored procedure.

    ! This is an exploratory development script.
    ! Not production-ready.
    ! Not optimized for performance.
    ! Used strictly for validation and transformation design.

-------------------------------------------------------------------------------
Key Validation Areas Covered:
-------------------------------------------------------------------------------

1. Date of Birth (bdate) Validation
    - Identify unrealistic or out-of-range birth dates.
    - Detect future dates (bdate > GETDATE()).
    - Detect extremely old dates (e.g., before 1924-01-01).
    - Validate need for NULL replacement in Silver layer.

    Business Rationale:
        Prevent inaccurate age calculations and reporting distortions.
        Ensure demographic analysis integrity.

2. Gender (gen) Consistency Analysis
    - Review distinct values stored in 'gen'.
    - Detect inconsistent coding patterns (F, FEMALE, M, MALE, etc.).
    - Prototype CASE-based standardization logic.
    - Define clean mapping strategy for Silver layer.

    Objective:
        Standardize categorical values for reporting clarity
        and consistent dimensional modeling.

3. Customer ID (cid) Cleansing Analysis
    - Detect prefixed values (e.g., 'NAS%').
    - Prototype logic to remove unwanted prefixes.
    - Validate substring extraction logic.
    - Confirm need for standardized customer identifiers.

    Purpose:
        Ensure consistent key structure before joining
        with CRM or other ERP tables in Silver layer.

-------------------------------------------------------------------------------
ETL Lifecycle Context:
-------------------------------------------------------------------------------

This script represents the Data Profiling phase within the Bronze layer:

    Source System → Bronze (Raw ERP Data)
                   → Profiling & Validation (This Script)
                   → Silver (Cleansed & Standardized Data)

All transformation rules later implemented in the Silver layer
were derived from findings in this exploratory analysis.

-------------------------------------------------------------------------------
Why This Step Is Critical:
-------------------------------------------------------------------------------

Without profiling:

    - Invalid birthdates could distort age analytics.
    - Inconsistent gender values could fragment reports.
    - Non-standardized customer IDs could break joins.
    - Data quality issues would propagate downstream.

This validation step ensured that:

     Business rules were evidence-based
     Cleansing logic was justified
     Transformations were tested before automation
     Silver layer design was data-driven

-------------------------------------------------------------------------------
Development Status:
-------------------------------------------------------------------------------

     Executed during warehouse build phase
     Used to define Silver layer transformation logic
     Helped design data standardization rules
     Not part of automated ETL pipeline
     Not scheduled for recurring execution

-------------------------------------------------------------------------------
*/

SELECT TOP (1000) [cid]
      ,[bdate]
      ,[gen]
  FROM [DataWarehouse].[bronze].[erp_cust_az12]

-- Identify out-of-range dates or very old customers
select distinct bdate
from bronze.erp_cust_az12
where bdate<'1924-01-01'
or bdate>GETDATE()

-- Data consistency in 'gen'
select distinct gen
from bronze.erp_cust_az12

select distinct gen,
case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
when upper(trim(gen)) in ('M','MALE') then 'Male'
else 'N/A'
end as clean_gen
from bronze.erp_cust_az12

-- Cleaning 'cid' of 'NAS%'
select cid,
case when cid like 'NAS%' then SUBSTRING(cid,4,LEN(cid))
else cid
end as clean_cid
from bronze.erp_cust_az12