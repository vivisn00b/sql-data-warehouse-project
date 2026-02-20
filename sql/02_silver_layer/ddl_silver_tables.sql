/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Purpose:
    Recreate all tables in the 'silver' schema with enterprise-level metadata.
===============================================================================
*/

/*==============================================================================
  CRM CUSTOMER INFO
==============================================================================*/

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id                INT            NOT NULL,
    cst_key               NVARCHAR(50)   NOT NULL,
    cst_firstname         NVARCHAR(50),
    cst_lastname          NVARCHAR(50),
    cst_marital_status    NVARCHAR(50),
    cst_gndr              NVARCHAR(50),
    cst_create_date       DATE,

    -- Metadata
    dwh_load_datetime     DATETIME2      DEFAULT SYSDATETIME(),
    dwh_source_system     NVARCHAR(50)   NULL,
    dwh_batch_id          INT,
);
GO

/*==============================================================================
  CRM PRODUCT INFO
==============================================================================*/

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id                INT            NOT NULL,
    cat_id                NVARCHAR(50),
    prd_key               NVARCHAR(50)   NOT NULL,
    prd_nm                NVARCHAR(100),
    prd_cost              DECIMAL(18,2),
    prd_line              NVARCHAR(50),
    prd_start_dt          DATE,
    prd_end_dt            DATE,

    -- Metadata
    dwh_load_datetime     DATETIME2      DEFAULT SYSDATETIME(),
    dwh_source_system     NVARCHAR(50)   NULL,
    dwh_batch_id          INT,
);
GO

/*==============================================================================
  CRM SALES DETAILS
==============================================================================*/

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num           NVARCHAR(50)   NOT NULL,
    sls_prd_key           NVARCHAR(50)   NOT NULL,
    sls_cust_id           INT            NOT NULL,
    sls_order_dt          DATE,
    sls_ship_dt           DATE,
    sls_due_dt            DATE,
    sls_sales             DECIMAL(18,2),
    sls_quantity          INT,
    sls_price             DECIMAL(18,2),

    -- Metadata
    dwh_load_datetime     DATETIME2      DEFAULT SYSDATETIME(),
    dwh_source_system     NVARCHAR(50)   NULL,
    dwh_batch_id          INT,
);
GO

/*==============================================================================
  ERP LOCATION
==============================================================================*/

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid                   NVARCHAR(50)   NOT NULL,
    cntry                 NVARCHAR(50),

    -- Metadata
    dwh_load_datetime     DATETIME2      DEFAULT SYSDATETIME(),
    dwh_source_system     NVARCHAR(50)   NULL,
    dwh_batch_id          INT,
);
GO

/*==============================================================================
  ERP CUSTOMER
==============================================================================*/

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid                   NVARCHAR(50)   NOT NULL,
    bdate                 DATE,
    gen                   NVARCHAR(50),

    -- Metadata
    dwh_load_datetime     DATETIME2      DEFAULT SYSDATETIME(),
    dwh_source_system     NVARCHAR(50)   NULL,
    dwh_batch_id          INT,
);
GO

/*==============================================================================
  ERP PRODUCT CATEGORY
==============================================================================*/

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id                    NVARCHAR(50)   NOT NULL,
    cat                   NVARCHAR(50),
    subcat                NVARCHAR(50),
    maintenance           NVARCHAR(50),

    -- Metadata
    dwh_load_datetime     DATETIME2      DEFAULT SYSDATETIME(),
    dwh_source_system     NVARCHAR(50)   NULL,
    dwh_batch_id          INT,
);
GO