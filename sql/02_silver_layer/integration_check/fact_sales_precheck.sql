SELECT TOP (1000) [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
      ,[dwh_load_datetime]
      ,[dwh_source_system]
      ,[dwh_batch_id]
  FROM [DataWarehouse].[silver].[crm_sales_details]

SELECT TOP (1000) [customer_key]
      ,[customer_id]
      ,[customer_number]
      ,[first_name]
      ,[last_name]
      ,[country]
      ,[marital_status]
      ,[gender]
      ,[birthdate]
      ,[create_date]
  FROM [DataWarehouse].[gold].[dim_customers]

SELECT TOP (1000) [product_key]
      ,[product_number]
      ,[product_id]
      ,[product_name]
      ,[category_id]
      ,[category]
      ,[subcategory]
      ,[maintenance]
      ,[cost]
      ,[product_line]
      ,[start_date]
  FROM [DataWarehouse].[gold].[dim_products]

-- Found NULL in birthdate column of gold.dim_customers
SELECT *
FROM gold.dim_customers
WHERE birthdate is null

-- Check if they're NULL in silver layer or not
select *
from silver.erp_cust_az12
where cid IN (
    SELECT customer_number
    FROM gold.dim_customers
    WHERE birthdate is null
)

-- Found them NULL in silver layer, check if they are NULL in bronze layer
SELECT *
FROM bronze.erp_cust_az12
WHERE 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END 
IN (
    SELECT customer_number
    FROM gold.dim_customers
    WHERE birthdate is null
);  -- bdate > current date so no action taken

-- Final query
SELECT sd.sls_ord_num, sd.sls_prd_key,
       sd.sls_cust_id, sd.sls_order_dt,
       sd.sls_ship_dt, sd.sls_due_dt,
       sd.sls_sales, sd.sls_quantity,
       sd.sls_price,
       pr.product_key,
       cu.customer_key
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_id
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id
