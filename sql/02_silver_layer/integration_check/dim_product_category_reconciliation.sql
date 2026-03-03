/*
===============================================================================
Script: Silver Layer Category Reconciliation & Fallback Mapping – Dim Product
Layer : Silver (Data Quality Remediation & Integration Enhancement)
===============================================================================

Overview:
    This script was developed after detecting NULL category and subcategory
    values during the Dim_Product precheck phase.

    Initial LEFT JOIN validation between:
        • silver.crm_prd_info
        • silver.erp_px_cat_g1v2

    revealed missing or unmatched category mappings between CRM and ERP systems.

    This script explores reconciliation strategies and implements fallback
    matching logic to improve dimensional completeness before promoting
    the model to the Gold layer.

    ! Not production code
    ! Not optimized for performance
    ! Not part of automated ETL execution
    ! Used strictly for development and validation purposes

-------------------------------------------------------------------------------
Problem Identified:
-------------------------------------------------------------------------------

    - Some CRM cat_id values do not exist in ERP reference table.
    - LEFT JOIN resulted in NULL category/subcategory values.
    - Potential many-to-many joins introduced by fallback matching logic.
    - Dim_Product risked incomplete classification in Gold layer.

-------------------------------------------------------------------------------
Objectives:
-------------------------------------------------------------------------------

1. Referential Integrity Analysis
    - Identify CRM category IDs not found in ERP.
    - Identify ERP category IDs not referenced in CRM.
    - Use FULL OUTER JOIN to detect unmatched records.

2. Key Structure Analysis
    - Decompose cat_id into component parts (category/subcategory).
    - Validate structural consistency using string parsing.

3. Fallback Matching Strategy
    - Step 1: Direct match (pi.cat_id = cg.id)
    - Step 2: Fallback match using business rule:
              LEFT(prd_key,2) = RIGHT(erp_id,2)
    - Step 3: Default to 'N/A' when no rule satisfies.

4. Many-to-Many Risk Detection
    - Detect duplicate product rows introduced by OR join condition.
    - Validate that fallback logic does not create data amplification.

5. Data Quality Improvement Design
    - Improve final_cat_id completeness.
    - Reduce NULL category values.
    - Document reconciliation assumptions before Gold promotion.

-------------------------------------------------------------------------------
ETL Methodology Context:
-------------------------------------------------------------------------------

This script represents the "Data Remediation & Rule Design" phase:

    Source → Bronze → Silver → Data Quality Fix → Gold

Key Principle:
    Gold dimensions must be fully classified and deterministic.
    All fallback logic must be validated for uniqueness and stability
    before being embedded into dimensional models.

-------------------------------------------------------------------------------
Governance & Design Notes:
-------------------------------------------------------------------------------

    Developed in response to Dim_Product precheck findings
    Documents cross-system reconciliation strategy
    Validates fallback join logic behavior
    Highlights potential many-to-many join risks
    Not scheduled
    Not parameterized
    Not productionized

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

-- check which cat id is not in crm_prd_info
select cat_id 
from silver.crm_prd_info
where cat_id NOT IN (
select id from silver.erp_px_cat_g1v2
)

-- Get joined table of all category
select pi.cat_id as crm_cat_id,
       cg.id as erp_cat_id,
       pi.prd_key,
       cg.cat, cg.subcat, pi.prd_nm 
from silver.crm_prd_info pi
full outer join silver.erp_px_cat_g1v2 cg
on pi.cat_id = cg.id

-- Get only unmatched rows
select *
from (
select pi.cat_id as crm_cat_id,
       cg.id as erp_cat_id, 
       cg.cat, cg.subcat, pi.prd_nm 
from silver.crm_prd_info pi
full outer join silver.erp_px_cat_g1v2 cg
on pi.cat_id = cg.id
)t where crm_cat_id is null or erp_cat_id is null

-- Extract cat and subcat from cat_id of silver.crm_prd_info table
SELECT  *,
            LEFT(cat_id, CHARINDEX('_', cat_id) - 1) AS cat_code,
            RIGHT(cat_id, LEN(cat_id) - CHARINDEX('_', cat_id)) AS subcat_code
    FROM silver.crm_prd_info

/*
Check for a direct match between cat_id and id first—if found, use it.
Otherwise, fallback to a match on the first two letters of the id.
If that matches, proceed to compare the first two letters of prd_nm.
If both checks pass, assign the cat_id from ERP; otherwise, assign N/A.
This ensures quick matches take precedence, while fallback rules handle edge cases gracefully.
*/ 

SELECT 
    pi.prd_id, pi.cat_id, pi.prd_key, pi.prd_nm, pi.prd_cost, pi.prd_line, pi.prd_start_dt,
    cg.id, cg.cat, cg.subcat, cg.maintenance,
    CASE
        WHEN pi.cat_id = cg.id THEN cg.id
        WHEN pi.cat_id <> cg.id 
             AND LEFT(pi.prd_key,2) = RIGHT(cg.id,2)
             THEN cg.id
        ELSE 'N/A'
    END AS final_cat_id
FROM silver.crm_prd_info pi
FULL OUTER JOIN silver.erp_px_cat_g1v2 cg
    ON pi.cat_id = cg.id
WHERE pi.prd_end_dt IS NULL

SELECT 
    pi.prd_id, pi.cat_id, pi.prd_key, pi.prd_nm, pi.prd_cost, pi.prd_line, pi.prd_start_dt,
    COALESCE(
        -- Step 1: exact cat_id match
        (SELECT id FROM silver.erp_px_cat_g1v2 e WHERE e.id = pi.cat_id),
        -- Step 2: fallback on prd_key first 2 vs id last 2
        (SELECT id FROM silver.erp_px_cat_g1v2 e 
         WHERE LEFT(pi.prd_key,2) = RIGHT(e.id,2)),
        -- Step 3: default
        'N/A'
    ) AS final_cat_id
FROM silver.crm_prd_info pi
WHERE pi.prd_end_dt IS NULL;

-- Test query
SELECT pi.prd_id, pi.cat_id, pi.prd_key, pi.prd_nm, pi.prd_cost, pi.prd_line, pi.prd_start_dt,
    cg.cat, cg.subcat, cg.maintenance,
    CASE
        WHEN pi.cat_id = cg.id THEN cg.id
        WHEN LEFT(pi.prd_key,2) = RIGHT(cg.id,2) THEN cg.id
        ELSE 'N/A'
    END AS final_cat_id
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 cg
    ON pi.cat_id = cg.id 
       OR LEFT(pi.prd_key,2) = RIGHT(cg.id,2)
WHERE pi.prd_end_dt IS NULL
and prd_id = 514;

-- Checking for duplicates
SELECT pi.prd_id, COUNT(*) AS cnt
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 cg
    ON pi.cat_id = cg.id
       OR (LEFT(pi.prd_key,2) = RIGHT(cg.id,2))
WHERE pi.prd_end_dt IS NULL
GROUP BY pi.prd_id
HAVING COUNT(*) > 1

SELECT 
    pi.prd_id,pi.cat_id, pi.prd_key, pi.prd_nm, pi.prd_cost, pi.prd_line, pi.prd_start_dt,
    cg.cat, cg.subcat, cg.maintenance,
    CASE
        WHEN pi.cat_id = cg.id THEN cg.id
        WHEN LEFT(pi.prd_key,2) = RIGHT(cg.id,2) THEN cg.id
        ELSE 'N/A'
    END AS final_cat_id
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 cg
    ON pi.cat_id = cg.id 
       OR LEFT(pi.prd_key,2) = RIGHT(cg.id,2)
WHERE pi.prd_end_dt IS NULL
and pi.prd_id IN (
    SELECT pi.prd_id
    FROM silver.crm_prd_info pi
    LEFT JOIN silver.erp_px_cat_g1v2 cg
        ON pi.cat_id = cg.id
           OR (LEFT(pi.prd_key,2) = RIGHT(cg.id,2))
    WHERE pi.prd_end_dt IS NULL
    GROUP BY pi.prd_id
    HAVING COUNT(*) > 1
)  -- many to many explosions

-- Final query
SELECT
    ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key, -- Surrogate key
    pi.prd_id       AS product_number,
    pi.prd_key      AS product_id,
    pi.prd_nm       AS product_name,
    pi.cat_id       AS category_id,
    pc.cat          AS category,
    COALESCE(
        -- Step 1: exact cat_id match
        (SELECT cat FROM silver.erp_px_cat_g1v2 e WHERE e.id = pi.cat_id),
        -- Step 2: fallback on prd_key first 2 vs id last 2 if cat_id first 2 = id first 2
        (SELECT cat FROM silver.erp_px_cat_g1v2 e 
         WHERE LEFT(pi.cat_id,2) = LEFT(e.id,2) 
         AND LEFT(pi.prd_key,2) = RIGHT(e.id,2)),
        -- STEP 3: fallback on cat_id first 2 vs cat first 2 if cat_id last 2 = id last 2
        (SELECT cat FROM silver.erp_px_cat_g1v2 e
         WHERE RIGHT(pi.cat_id,2) = RIGHT(e.id,2)
         AND LEFT(pi.cat_id,2) = UPPER(LEFT(e.cat,2))),
        -- Step 4: default
        'n/a'
    ) AS final_cat,
    pc.subcat       AS subcategory,
    COALESCE(
        -- Step 1: exact cat_id match
        (SELECT subcat FROM silver.erp_px_cat_g1v2 e WHERE e.id = pi.cat_id),
        -- Step 2: fallback on prd_key first 2 vs id last 2 if cat_id first 2 = id first 2
        (SELECT subcat FROM silver.erp_px_cat_g1v2 e 
         WHERE LEFT(pi.cat_id,2) = LEFT(e.id,2) 
         AND LEFT(pi.prd_key,2) = RIGHT(e.id,2)),
        -- STEP 3: fallback on cat_id first 2 vs cat first 2 if cat_id last 2 = id last 2
        (SELECT subcat FROM silver.erp_px_cat_g1v2 e
         WHERE RIGHT(pi.cat_id,2) = RIGHT(e.id,2)
         AND LEFT(pi.cat_id,2) = UPPER(LEFT(e.cat,2))),
        -- Step 4: default
        'n/a'
    ) AS final_subcat,
    pc.maintenance  AS maintenance,
    COALESCE(
    -- Step 1: exact cat_id match
    (SELECT maintenance FROM silver.erp_px_cat_g1v2 e WHERE e.id = pi.cat_id),
    -- Step 2: fallback on prd_key first 2 vs id last 2 if cat_id first 2 = id first 2
    (SELECT maintenance FROM silver.erp_px_cat_g1v2 e 
     WHERE LEFT(pi.cat_id,2) = LEFT(e.id,2) 
     AND LEFT(pi.prd_key,2) = RIGHT(e.id,2)),
    -- STEP 3: fallback on cat_id first 2 vs cat first 2 if cat_id last 2 = id last 2
    (SELECT maintenance FROM silver.erp_px_cat_g1v2 e
     WHERE RIGHT(pi.cat_id,2) = RIGHT(e.id,2)
     AND LEFT(pi.cat_id,2) = UPPER(LEFT(e.cat,2))),
    -- Step 4: default
    'n/a'
) AS final_maintenance,
    pi.prd_cost     AS cost,
    pi.prd_line     AS product_line,
    pi.prd_start_dt AS start_date,
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
    ) AS final_cat_id
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pi.cat_id = pc.id
WHERE pi.prd_end_dt IS NULL; -- Filter out all historical data
-- Assumptions: 1. ERP table as the absolute source of truth
--              2. Product cat id will always be in ERP but might have mismatch
--              3. First 2 letter of cat id is from first two letter of category
--              4. Next 2 letters of cat id are from subcat but will match with first two letter of prd id in case of mismatch