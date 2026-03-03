/*
===============================================================================
Script: Silver Layer Data Validation & Integration Check – Dim Customer
Layer : Silver (Pre-Gold Modeling Validation)
===============================================================================

Overview:
    This script contains exploratory SQL queries executed against Silver layer
    customer-related tables prior to creating the Gold layer Dim_Customer view.

    It was used to validate join logic, detect duplicate amplification,
    assess cross-system data consistency, and prototype transformation logic
    before implementing the final business-ready model in the Gold layer.

    The script reflects iterative integration validation and is intentionally
    exploratory in nature.

    ! Not production code
    ! Not optimized for performance
    ! Not part of automated ETL execution
    ! Used strictly for development and validation purposes

-------------------------------------------------------------------------------
Primary Goals:
-------------------------------------------------------------------------------

1. Source Review
    - Inspect CRM and ERP customer datasets.
    - Validate key columns and structural consistency.
    - Confirm expected row counts and load metadata.

2. Join Validation
    - Test LEFT JOIN logic between:
        • silver.crm_cust_info
        • silver.erp_cust_az12
        • silver.erp_loc_a101
    - Ensure customer key relationships behave as expected.
    - Detect unintended row multiplication.

3. Duplicate Detection
    - Group by cst_id after joins.
    - Identify duplicate records introduced by integration logic.
    - Validate one-to-one vs one-to-many relationships.

4. Cross-System Attribute Reconciliation
    - Compare gender fields (cst_gndr vs gen).
    - Identify mismatches and NULL inconsistencies.
    - Evaluate data quality differences between source systems.

5. Business Rule Prototyping
    - Assume CRM as master source for customer attributes.
    - Design CASE-based fallback logic using COALESCE().
    - Prototype standardized gender field (new_gender).

6. Gold Layer Preparation
    - Validate transformation assumptions.
    - Reduce risk before creating Dim_Customer view.
    - Ensure clean, deduplicated, business-aligned output.

-------------------------------------------------------------------------------
ETL Methodology Context:
-------------------------------------------------------------------------------

This script represents the "Data Validation & Integration" phase:

    Source → Bronze → Silver (Cleansed & Integrated) → Validation → Gold

Key Principle:
    Gold layer models must be stable, deduplicated, and business-aligned.
    All integration logic must be validated before promotion to Gold.

Why This Matters:
    - Prevents duplicate customer records in analytics layer.
    - Ensures consistent attribute selection across systems.
    - Protects downstream BI reports from data inconsistencies.
    - Documents integration decisions for governance purposes.

-------------------------------------------------------------------------------
Development & Governance Notes:
-------------------------------------------------------------------------------

    Executed during warehouse build phase
    Informed Gold layer Dim_Customer design
    Helped validate integration assumptions
    Documents attribute selection strategy
    Not scheduled
    Not parameterized
    Not part of production pipeline

-------------------------------------------------------------------------------
*/

SELECT TOP (1000) [cst_id]
      ,[cst_key]
      ,[cst_firstname]
      ,[cst_lastname]
      ,[cst_marital_status]
      ,[cst_gndr]
      ,[cst_create_date]
      ,[dwh_load_datetime]
      ,[dwh_source_system]
      ,[dwh_batch_id]
  FROM [DataWarehouse].[silver].[crm_cust_info]

SELECT TOP (1000) [cid]
      ,[bdate]
      ,[gen]
      ,[dwh_load_datetime]
      ,[dwh_source_system]
      ,[dwh_batch_id]
  FROM [DataWarehouse].[silver].[erp_cust_az12]

SELECT TOP (1000) [cid]
      ,[cntry]
      ,[dwh_load_datetime]
      ,[dwh_source_system]
      ,[dwh_batch_id]
  FROM [DataWarehouse].[silver].[erp_loc_a101]

SELECT ci.cst_id
      ,ci.cst_key
      ,ci.cst_firstname
      ,ci.cst_lastname
      ,ci.cst_marital_status
      ,ci.cst_gndr
      ,ci.cst_create_date
      ,ca.bdate
      ,ca.gen
      ,la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

-- After joining table check if any duplicates were introduced by the join logic
select cst_id, count(*)
from (
SELECT ci.cst_id
      ,ci.cst_key
      ,ci.cst_firstname
      ,ci.cst_lastname
      ,ci.cst_marital_status
      ,ci.cst_gndr
      ,ci.cst_create_date
      ,ca.bdate
      ,ca.gen
      ,la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
)t group by cst_id
having count(*)>1

-- Data Integration for cst_gndr and gen columns
SELECT distinct ci.cst_gndr
      ,ca.gen     
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
-- might also get NULL if there is no match on other table

-- Fix assuming CRM is the master source of data
SELECT distinct ci.cst_gndr, ca.gen,
case when ci.cst_gndr <> 'n/a' then ci.cst_gndr
     when ci.cst_gndr is NULL or ci.cst_gndr = 'n/a' then coalesce(ca.gen,'n/a')
     else coalesce(cst_gndr,'n/a')
end as new_gender
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

-- Final query
SELECT
    ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key, -- Surrogate key
    pi.prd_id       AS product_number,
    pi.prd_key      AS product_id,
    pi.prd_nm       AS product_name,
    COALESCE(
        -- Step 1: exact cat_id match
        (SELECT id FROM silver.erp_px_cat_g1v2 e WHERE e.id = pi.cat_id),
        -- Step 2: fallback on prd_key first 2 vs id last 2 if cat_id first 2 = id first 2
        (SELECT id FROM silver.erp_px_cat_g1v2 e 
         WHERE LEFT(pi.cat_id,2) = LEFT(e.id,2) 
         AND LEFT(pi.prd_key,2) = RIGHT(e.id,2)),
        -- STEP 3: fallback on cat_id first 2 vs cat first 2 if cat_id last 2 = id last 2
        (SELECT id FROM silver.erp_px_cat_g1v2 e
         WHERE RIGHT(pi.cat_id,2) = RIGHT(e.id,2)
         AND LEFT(pi.cat_id,2) = UPPER(LEFT(e.cat,2))),
        -- Step 4: default
        'n/a'
    ) AS category_id,
    COALESCE(
        (SELECT cat FROM silver.erp_px_cat_g1v2 e WHERE e.id = pi.cat_id),
        (SELECT cat FROM silver.erp_px_cat_g1v2 e 
         WHERE LEFT(pi.cat_id,2) = LEFT(e.id,2) 
         AND LEFT(pi.prd_key,2) = RIGHT(e.id,2)),
        (SELECT cat FROM silver.erp_px_cat_g1v2 e
         WHERE RIGHT(pi.cat_id,2) = RIGHT(e.id,2)
         AND LEFT(pi.cat_id,2) = UPPER(LEFT(e.cat,2))),
        'n/a'
    ) AS category,
    COALESCE(
        (SELECT subcat FROM silver.erp_px_cat_g1v2 e WHERE e.id = pi.cat_id),
        (SELECT subcat FROM silver.erp_px_cat_g1v2 e 
         WHERE LEFT(pi.cat_id,2) = LEFT(e.id,2) 
         AND LEFT(pi.prd_key,2) = RIGHT(e.id,2)),
        (SELECT subcat FROM silver.erp_px_cat_g1v2 e
         WHERE RIGHT(pi.cat_id,2) = RIGHT(e.id,2)
         AND LEFT(pi.cat_id,2) = UPPER(LEFT(e.cat,2))),
        'n/a'
    ) AS subcategory,
    COALESCE(
    (SELECT maintenance FROM silver.erp_px_cat_g1v2 e WHERE e.id = pi.cat_id),
    (SELECT maintenance FROM silver.erp_px_cat_g1v2 e 
     WHERE LEFT(pi.cat_id,2) = LEFT(e.id,2) 
     AND LEFT(pi.prd_key,2) = RIGHT(e.id,2)),
    (SELECT maintenance FROM silver.erp_px_cat_g1v2 e
     WHERE RIGHT(pi.cat_id,2) = RIGHT(e.id,2)
     AND LEFT(pi.cat_id,2) = UPPER(LEFT(e.cat,2))),
    'n/a'
) AS maintenance,
    pi.prd_cost     AS cost,
    pi.prd_line     AS product_line,
    pi.prd_start_dt AS start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pi.cat_id = pc.id
WHERE pi.prd_end_dt IS NULL; -- Filter out all historical data
GO

-- Optimized query
SELECT
    ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key, 
    pi.prd_id       AS product_number,
    pi.prd_key      AS product_id,
    pi.prd_nm       AS product_name,

    -- Category attributes from best match
    COALESCE(cat_match.id, 'n/a')          AS category_id,
    COALESCE(cat_match.cat, 'n/a')         AS category,
    COALESCE(cat_match.subcat, 'n/a')      AS subcategory,
    COALESCE(cat_match.maintenance, 'n/a') AS maintenance,

    pi.prd_cost     AS cost,
    pi.prd_line     AS product_line,
    pi.prd_start_dt AS start_date

FROM silver.crm_prd_info pi

OUTER APPLY (
    SELECT TOP 1
        e.id,
        e.cat,
        e.subcat,
        e.maintenance,

        -- Ranking logic to preserve your fallback priority
        CASE
            WHEN e.id = pi.cat_id THEN 1
            WHEN LEFT(pi.cat_id,2) = LEFT(e.id,2) 
                 AND LEFT(pi.prd_key,2) = RIGHT(e.id,2) THEN 2
            WHEN RIGHT(pi.cat_id,2) = RIGHT(e.id,2)
                 AND LEFT(pi.cat_id,2) = UPPER(LEFT(e.cat,2)) THEN 3
            ELSE 4
        END AS match_priority

    FROM silver.erp_px_cat_g1v2 e
    WHERE
        e.id = pi.cat_id
        OR (LEFT(pi.cat_id,2) = LEFT(e.id,2) 
            AND LEFT(pi.prd_key,2) = RIGHT(e.id,2))
        OR (RIGHT(pi.cat_id,2) = RIGHT(e.id,2)
            AND LEFT(pi.cat_id,2) = UPPER(LEFT(e.cat,2)))

    ORDER BY match_priority
) cat_match

WHERE pi.prd_end_dt IS NULL;
GO

--Before (Correlated Subqueries)
--For each column (id, cat, subcat, maintenance), SQL Server ran separate subqueries.
--That means repeated scans, higher CPU, poor scaling
--Now (OUTER APPLY)
--For each product: one lookup, one ranking, one result returned
