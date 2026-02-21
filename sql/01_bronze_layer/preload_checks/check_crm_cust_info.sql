/*
===============================================================================
Script: Bronze Layer Data Validation & Cleansing Analysis (Ad-Hoc Script)
Layer : Bronze (Pre-Silver Validation)
===============================================================================

Purpose:
    This script represents exploratory and validation queries performed on the
    Bronze layer before designing and implementing the Silver load procedure.

    It is intentionally written as an ad-hoc, investigative script used for:
        - Data profiling
        - Data quality validation
        - Duplicate detection
        - Null analysis
        - Standardization checks
        - Cleansing logic validation

    ! This is NOT a production stored procedure.
    ! This is a messy working script used for data investigation and validation.
    ! It was used to understand data issues before building transformation logic
      inside the Silver layer.

-------------------------------------------------------------------------------
What This Script Helped Identify:
-------------------------------------------------------------------------------

1. Primary Key Issues
    - Detection of NULL values in cst_id.
    - Detection of duplicate customer IDs.
    - Validation of uniqueness assumptions.
    - Helped determine need for ROW_NUMBER() logic in Silver layer.

2. Duplicate Handling Strategy
    - Used ROW_NUMBER() partitioned by cst_id.
    - Ordered by cst_create_date DESC to retain most recent record.
    - Identified which records should be kept vs removed.

3. Data Cleansing Observations
    - Leading/trailing spaces in:
        • cst_firstname
        • cst_lastname
        • cst_gndr
        • cst_marital_status
    - Confirmed need for TRIM() in Silver transformations.

4. Data Standardization Issues
    - Inconsistent gender values (M, F, etc.)
    - Inconsistent marital status codes.
    - Confirmed need for CASE normalization logic in Silver layer.

5. Final Validation Query
    - Simulated the final cleaned dataset.
    - Ensured business logic produced expected results.
    - Acted as a prototype before moving logic into stored procedure.

-------------------------------------------------------------------------------
Why This Step Is Important in ETL:
-------------------------------------------------------------------------------

Data Warehousing Best Practice:
    Never directly transform data without profiling it first.

Bronze Layer Goals:
    - Preserve raw source data.
    - Perform investigation and quality assessment.
    - Identify structural and business rule issues.

This script represents the "Data Understanding & Profiling" phase
of the ETL lifecycle before transformation logic was formalized
in the Silver layer stored procedure.

-------------------------------------------------------------------------------
Status:
    Used during development
    Supports Silver layer transformation design
    Not intended for production execution
-------------------------------------------------------------------------------
*/

SELECT TOP (1000) [cst_id]
      ,[cst_key]
      ,[cst_firstname]
      ,[cst_lastname]
      ,[cst_marital_status]
      ,[cst_gndr]
      ,[cst_create_date]
  FROM [DataWarehouse].[bronze].[crm_cust_info]

-- Check for NULLs or Duplicates in Primary Key
  select cst_id,
  COUNT(*)
  from bronze.crm_cust_info
  group by cst_id
  having count(*) > 1 OR cst_id is null

  SELECT
       [cst_id]
      ,[cst_key]
      ,[cst_firstname]
      ,[cst_lastname]
      ,[cst_marital_status]
      ,[cst_gndr]
      ,[cst_create_date]
  FROM [DataWarehouse].[bronze].[crm_cust_info]
  where cst_id IN (
  select cst_id
  from bronze.crm_cust_info
  group by cst_id
  having count(*) > 1 OR cst_id is null
  )

SELECT *,
ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_latest_date
FROM bronze.crm_cust_info
where cst_id IN (
  select cst_id
  from bronze.crm_cust_info
  group by cst_id
  having count(*) > 1
  ) OR cst_id is null

SELECT *
from (
select *,
ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_latest_date
FROM bronze.crm_cust_info
)t where flag_latest_date <> 1

SELECT *
from (
select *,
ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_latest_date
FROM bronze.crm_cust_info
)t where flag_latest_date = 1 and cst_id is not null

-- Check for Unwanted Spaces
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

select cst_firstname, cst_lastname
from bronze.crm_cust_info
where cst_firstname <> TRIM(cst_firstname)
or cst_lastname <> TRIM(cst_lastname)

select cst_gndr, cst_marital_status
from bronze.crm_cust_info
where cst_gndr != TRIM(cst_gndr) or cst_marital_status != trim(cst_marital_status)

-- Data Standardization & Consistency Check
SELECT distinct cst_marital_status
from bronze.crm_cust_info

SELECT distinct cst_gndr
from bronze.crm_cust_info

-- Final
select cst_id, cst_key,
trim(cst_firstname) cst_firstname,
trim(cst_lastname) cst_lastname,
Case upper(trim(cst_gndr))
    when 'F' THEN 'Female'
    when 'M' THEN 'Male'
    ELSE 'N/A'
END cst_gndr,
cst_create_date
from (
select *,
ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_latest_date
FROM bronze.crm_cust_info
)t where flag_latest_date = 1 and cst_id is not null
