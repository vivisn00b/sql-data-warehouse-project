/*
===============================================================================
Stored Procedure: Run Full ETL Load (Bronze -> Silver)
===============================================================================
Script Purpose:
    - Orchestrates the full ETL load, executing Bronze and Silver layers sequentially.
    - Tracks batch execution metadata in etl.batch_control.
    - Captures start and end times, status (Running / Success / Failed), and error messages.

Tables Used / Updated:
    - etl.batch_control (logs batch metadata)
    - Bronze layer tables (via bronze.load_bronze)
    - Silver layer tables (via silver.load_silver)

Parameters:
    - None (internally generates @batch_id for layer procedures)

Usage:
    EXEC etl.run_full_load;
-------------------------------------------------------------------------------
*/

CREATE OR ALTER PROCEDURE etl.run_full_load
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @batch_id BIGINT,
        @layer_start DATETIME,
        @layer_end DATETIME,
        @start_time DATETIME = GETDATE();

    -- Generate dynamic batch ID (YYYYMMDDHHMMSS format)
    SET @batch_id = CAST(FORMAT(@start_time,'yyyyMMddHHmmss') AS BIGINT);

    BEGIN TRY

        ------------------------------------------------------
        -- Load Bronze Layer
        ------------------------------------------------------
        SET @layer_start = GETDATE();

        -- Insert Batch Start Record
        INSERT INTO etl.batch_control (
            batch_id,
            batch_start,
            batch_status,
            batch_layer
        )
        VALUES (
            @batch_id,
            @layer_start,
            'Running',
            'Bronze'
        );

        -- Run Bronze load
        EXEC bronze.load_bronze;

        -- Update Bronze as success
        SET @layer_end = GETDATE();
        UPDATE etl.batch_control
        SET batch_end = @layer_end, batch_status = 'Success'
        WHERE batch_id = @batch_id AND batch_layer = 'Bronze';

        PRINT 'Bronze Layer Completed. Duration: ' 
              + CAST(DATEDIFF(SECOND,@layer_start,@layer_end) AS VARCHAR) + ' seconds';

        ------------------------------------------------------
        -- Load Silver Layer
        ------------------------------------------------------
        SET @layer_start = GETDATE();

        -- Insert control row for Silver
        INSERT INTO etl.batch_control (
            batch_id,
            batch_start,
            batch_status,
            batch_layer
        )
        VALUES (
            @batch_id,
            @layer_start,
            'Running',
            'Silver'
        );

        -- Run Silver load
        EXEC silver.load_silver @batch_id;

        -- Update Silver as success
        SET @layer_end = GETDATE();
        UPDATE etl.batch_control
        SET batch_end = @layer_end, batch_status = 'Success'
        WHERE batch_id = @batch_id AND batch_layer = 'Silver';

        PRINT 'Silver Layer Completed. Duration: ' 
              + CAST(DATEDIFF(SECOND,@layer_start,@layer_end) AS VARCHAR) + ' seconds';
        
        PRINT 'Full Load Completed Successfully. Batch ID: ' + CAST(@batch_id AS VARCHAR);

    END TRY
    BEGIN CATCH
    
        -- Catch any error in Bronze or Silver
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();

        -- Update failed batch rows
        UPDATE etl.batch_control
        SET batch_end = GETDATE(),
            batch_status = 'Failed',
            error_message = @err_msg
        WHERE batch_id = @batch_id
          AND batch_end IS NULL;

        PRINT 'Full Load Failed. Batch ID: ' + CAST(@batch_id AS VARCHAR);
        PRINT 'Error: ' + @err_msg;

        THROW;

    END CATCH
END
GO