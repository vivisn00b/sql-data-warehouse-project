/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    - Loads cleansed and transformed data into the 'silver' schema.
    - Truncates silver tables before inserting transformed data.
    - Applies data cleaning, normalization, and business logic rules.

Tables Loaded:
    - CRM: crm_cust_info, crm_prd_info, crm_sales_details
    - ERP: erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2

Parameters:
    - @batch_id BIGINT : The unique batch ID assigned by the parent ETL process.

Usage:
    DECLARE @batch_id BIGINT = 20260301123000;
    EXEC silver.load_silver @batch_id;

-------------------------------------------------------------------------------
Learning Notes:
-------------------------------------------------------------------------------
1. Silver Layer Concept
    - Bronze = Raw data (as-is from source)
    - Silver = Cleaned, standardized, business-ready data

2. Data Normalization
    - Converts coded values (M/F, S/M) into readable values.
    - Improves reporting clarity and usability.

3. Data Validation Logic
    - Recalculates incorrect sales values.
    - Handles invalid or missing dates.
    - Protects analytical integrity.

4. Timing Variables
    - @batch_start_time / @batch_end_time → Total duration
    - @start_time / @end_time → Per table duration

5. Transactions (BEGIN TRANSACTION / COMMIT / ROLLBACK)
    - BEGIN TRANSACTION:
        Starts a logical unit of work.
        Ensures all operations are treated as one atomic process.

    - COMMIT TRANSACTION:
        Saves all changes permanently if the entire process succeeds.

    - ROLLBACK TRANSACTION:
        Reverts all changes if an error occurs.
        Prevents partial loads and maintains data consistency.

    - IF @@TRANCOUNT > 0:
        Checks if a transaction is active before rolling back.
        Prevents additional errors during failure handling.

    Why Transactions Are Important in ETL:
        Without a transaction:
            If one table fails, previous tables remain modified.
            This leads to inconsistent or corrupted Silver data.

        With a transaction:
            Either ALL tables load successfully,
            OR nothing is changed (all changes are rolled back).

        This ensures Atomicity (A in ACID properties).

-------------------------------------------------------------------------------
*/

CREATE OR ALTER PROCEDURE silver.load_silver
	@batch_id BIGINT
AS
BEGIN

    --SET NOCOUNT ON;

    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY

        -------------------------------------------------------------------------
        -- START TRANSACTION (Atomic Load)
        -------------------------------------------------------------------------
        BEGIN TRANSACTION;

        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

        -------------------------------------------------------------------------
        -- CRM Tables
        -------------------------------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        -------------------------------------------------------------------------
        -- silver.crm_cust_info
        -------------------------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';

        INSERT INTO silver.crm_cust_info (
            cst_id, 
            cst_key, 
            cst_firstname, 
            cst_lastname, 
            cst_marital_status, 
            cst_gndr,
            cst_create_date,
			dwh_source_system,
			dwh_batch_id      
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END,
            cst_create_date,
			'CRM',
			@batch_id
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id 
                       ORDER BY cst_create_date DESC
                   ) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -------------------------------------------------------------------------
        -- silver.crm_prd_info
        -------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt,
			dwh_source_system,
			dwh_batch_id
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt, -- Calculate end date as one day before the next start date
			'CRM',
			@batch_id
		FROM bronze.crm_prd_info;
        
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-------------------------------------------------------------------------
        -- silver.crm_sales_details
        -------------------------------------------------------------------------
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price,
			dwh_source_system,
			dwh_batch_id
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE
				WHEN sls_sales IS NULL THEN ABS(sls_quantity) * ABS(COALESCE(sls_price,0)) 
				WHEN sls_sales < 0 THEN ABS(sls_sales)
				WHEN sls_sales = 0 THEN ABS(sls_quantity) * ABS(COALESCE(sls_price,0)) 
				WHEN ABS(sls_quantity) * ABS(sls_price) != sls_sales THEN ABS(sls_quantity) * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Handle invaild data & recalculate if original value is missing or incorrect
			ABS(sls_quantity) AS sls_quantity,
			CASE 
				WHEN sls_price < 0 THEN ABS(sls_price)
				WHEN sls_price = 0 THEN NULLIF(sls_price, 0)
				WHEN sls_price IS NULL THEN sls_sales / ABS(sls_quantity)
				ELSE sls_price
			END AS sls_price, -- Derive price if original value is invaild
			'CRM',
			@batch_id
		FROM bronze.crm_sales_details;
        
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-------------------------------------------------------------------------
		-- silver.erp_cust_az12
		-------------------------------------------------------------------------	
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen,
			dwh_source_system,
			dwh_batch_id
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen, -- Normalize gender values and handle unknown cases
			'ERP',
			@batch_id
		FROM bronze.erp_cust_az12;
	    
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		-------------------------------------------------------------------------
		-- silver.erp_loc_a101
		-------------------------------------------------------------------------
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry,
			dwh_source_system,
			dwh_batch_id
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry, -- Normalize and Handle missing or blank country codes
			'ERP',
			@batch_id
		FROM bronze.erp_loc_a101;
	    
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-------------------------------------------------------------------------
		-- silver.erp_px_cat_g1v2
		-------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance,
			dwh_source_system,
			dwh_batch_id
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance,
			'ERP',
			@batch_id
		FROM bronze.erp_px_cat_g1v2;
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -------------------------------------------------------------------------
        -- COMMIT TRANSACTION (If everything succeeds)
        -------------------------------------------------------------------------
        COMMIT TRANSACTION;

        SET @batch_end_time = GETDATE();

        PRINT '==========================================';
        PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + 
              CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) 
              + ' seconds';
        PRINT '==========================================';

    END TRY

    BEGIN CATCH

        -------------------------------------------------------------------------
        -- ROLLBACK IF ERROR OCCURS
        -------------------------------------------------------------------------
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';

		THROW;
    END CATCH

END
GO
