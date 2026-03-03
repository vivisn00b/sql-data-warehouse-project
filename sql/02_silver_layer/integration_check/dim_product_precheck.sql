/*
===============================================================================
Script: Silver Layer Data Validation & Integration Check – Dim Product
Layer : Silver (Pre-Gold Modeling Validation)
===============================================================================

Overview:
    This script contains exploratory SQL queries executed against Silver layer
    product and category tables prior to creating the Gold layer Dim_Product view.

    It was used to validate active-product filtering logic, test join integrity
    between CRM and ERP systems, detect duplicate amplification, and assess
    category completeness before promoting the model to the Gold layer.

    The script reflects iterative integration validation and is intentionally
    exploratory in nature.

    ! Not production code
    ! Not optimized for performance
    ! Not part of automated ETL execution
    ! Used strictly for development and validation purposes

-------------------------------------------------------------------------------
Primary Goals:
-------------------------------------------------------------------------------

1. Source Inspection
    - Review CRM product data (crm_prd_info).
    - Review ERP category reference data (erp_px_cat_g1v2).
    - Validate structure and metadata columns.

2. Active Record Filtering
    - Exclude historical products using prd_end_dt IS NULL.
    - Ensure Gold layer reflects only current active products.
    - Validate slowly changing dimension (SCD) handling assumptions.

3. Join Integrity Validation
    - Test LEFT JOIN logic between product and category tables.
    - Confirm category key relationships behave as expected.
    - Detect unintended row multiplication.

4. Duplicate Detection
    - Group by prd_key after join.
    - Identify duplicate products introduced by join logic.
    - Validate uniqueness assumption for product business key.

5. Category Completeness Check
    - Identify products with NULL category or subcategory.
    - Detect CRM category IDs not found in ERP reference table.
    - Optionally detect unused ERP categories.

6. Gold Layer Readiness
    - Validate referential integrity.
    - Ensure complete dimensional attributes.
    - Reduce risk before creating Dim_Product view.

-------------------------------------------------------------------------------
ETL Methodology Context:
-------------------------------------------------------------------------------

This script represents the "Integration Validation" phase:

    Source → Bronze → Silver (Cleansed & Integrated) → Validation → Gold

Key Principle:
    Dimensional models must contain only active, deduplicated,
    and fully classified products before exposure to analytics.

Why This Matters:
    - Prevents orphaned category values in reports.
    - Protects BI models from incomplete hierarchies.
    - Ensures one-product-one-record behavior.
    - Documents integration and filtering assumptions.

-------------------------------------------------------------------------------
Development & Governance Notes:
-------------------------------------------------------------------------------

    Executed during warehouse build phase
    Informed Gold layer Dim_Product design
    Validates SCD filtering strategy
    Documents category reconciliation decisions
    Not scheduled
    Not parameterized
    Not part of production pipeline

-------------------------------------------------------------------------------
*/

SELECT TOP (1000) [prd_id]
      ,[cat_id]
      ,[prd_key]
      ,[prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_dt]
      ,[dwh_load_datetime]
      ,[dwh_source_system]
      ,[dwh_batch_id]
  FROM [DataWarehouse].[silver].[crm_prd_info]

SELECT TOP (1000) [id]
      ,[cat]
      ,[subcat]
      ,[maintenance]
      ,[dwh_load_datetime]
      ,[dwh_source_system]
      ,[dwh_batch_id]
  FROM [DataWarehouse].[silver].[erp_px_cat_g1v2]

-- Filter out all historical data and join with category table
SELECT pi.prd_id
      ,pi.cat_id
      ,pi.prd_key
      ,pi.prd_nm
      ,pi.prd_cost
      ,pi.prd_line
      ,pi.prd_start_dt
      ,cg.cat
      ,cg.subcat
      ,cg.maintenance
FROM silver.crm_prd_info pi
left join silver.erp_px_cat_g1v2 cg
On pi.cat_id = cg.id
where pi.prd_end_dt is null

-- Check for duplicates
select prd_key, count(*)
from (
SELECT pi.prd_id
      ,pi.cat_id
      ,pi.prd_key
      ,pi.prd_nm
      ,pi.prd_cost
      ,pi.prd_line
      ,pi.prd_start_dt
      ,cg.cat
      ,cg.subcat
      ,cg.maintenance
FROM silver.crm_prd_info pi
left join silver.erp_px_cat_g1v2 cg
On pi.cat_id = cg.id
where pi.prd_end_dt is null
)t group by prd_key
having COUNT(*)>1

-- Check for NUll categories
SELECT pi.prd_id
      ,pi.cat_id
      ,pi.prd_key
      ,pi.prd_nm
      ,pi.prd_cost
      ,pi.prd_line
      ,pi.prd_start_dt
      ,cg.cat
      ,cg.subcat
      ,cg.maintenance
FROM silver.crm_prd_info pi
left join silver.erp_px_cat_g1v2 cg
On pi.cat_id = cg.id
where pi.prd_end_dt is null
and cg.cat is null or cg.subcat is null

-- CRM values not found in ERP
SELECT DISTINCT pi.cat_id
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 cg
    ON pi.cat_id = cg.id
WHERE cg.id IS NULL;

-- ERP values not referenced in CRM (optional check)
SELECT DISTINCT cg.id
FROM silver.erp_px_cat_g1v2 cg
LEFT JOIN silver.crm_prd_info pi
    ON pi.cat_id = cg.id
WHERE pi.cat_id IS NULL;