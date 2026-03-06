/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.

-------------------------------------------------------------------------------
Learning Notes:
-------------------------------------------------------------------------------
1. OUTER APPLY (Advanced Row-Level Lookup)

   * OUTER APPLY executes a subquery for each row of the outer table.
   * It allows using TOP, ORDER BY, and complex conditional logic inside the subquery.
   * It returns NULL values when no match is found (similar to LEFT JOIN behavior).
   * Useful when matching logic requires prioritization or fallback rules.

   Why OUTER APPLY Was Used:
   - Needed prioritized category matching (exact match → partial match → fallback).
   - Required selecting TOP 1 match based on custom ranking logic.
   - Avoided multiple correlated subqueries for each column.
   - Improved readability and scalability.

2. OUTER APPLY vs LEFT JOIN

   LEFT JOIN:
   - Used for simple, static join conditions.
   - Best for direct key-based relationships (e.g., foreign key joins).
   - Cannot easily implement per-row ranking or TOP-based matching.

   OUTER APPLY:
   - Evaluates subquery separately for each row of the outer table.
   - Supports dynamic filtering and ranking logic.
   - Ideal for complex fallback matching scenarios.

   Key Difference:
   - LEFT JOIN = Static relationship between two tables.
   - OUTER APPLY = Row-by-row evaluated logic with flexible matching.

3. Surrogate Keys (Dimensional Modeling Concept)

   * A surrogate key is a system-generated unique identifier for dimension tables.
   * It is independent of business keys (e.g., customer_id, product_id).
   * Typically implemented using IDENTITY, SEQUENCE, or HASH.

   Why Surrogate Keys Are Used:
   - Ensures stable and consistent relationships between fact and dimension tables.
   - Protects the warehouse from changes in source system business keys.
   - Supports Slowly Changing Dimensions (SCD).
   - Improves join performance in star schemas.

-------------------------------------------------------------------------------
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key, -- Surrogate key
    pi.prd_id       AS product_number,
    pi.prd_key      AS product_id,
    pi.prd_nm       AS product_name,

    -- Category attributes from best match
    cat_match.id          AS category_id,
    cat_match.cat         AS category,
    cat_match.subcat      AS subcategory,
    cat_match.maintenance AS maintenance,

    pi.prd_cost     AS cost,
    pi.prd_line     AS product_line,
    pi.prd_start_dt AS start_date

FROM silver.crm_prd_info pi

OUTER APPLY (
    SELECT TOP 1  -- To ensure only one category is selected per product
        e.id,
        e.cat,
        e.subcat,
        e.maintenance,

        -- Ranking logic to preserve fallback priority
        -- Step 1: exact cat_id match
        -- Step 2: fallback on prd_key first 2 vs id last 2 if cat_id first 2 = id first 2
        -- STEP 3: fallback on cat_id first 2 vs cat first 2 if cat_id last 2 = id last 2
        -- Step 4: default
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

    ORDER BY match_priority  -- To ensure it is the best possible match
) cat_match

WHERE pi.prd_end_dt IS NULL;  -- Filter out all historical data
GO
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_id
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO
