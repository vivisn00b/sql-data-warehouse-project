/*
===============================================================================
Batch Control Table Setup
===============================================================================
Script Purpose:
    This script drops (if exists) and recreates the etl.batch_control table. 
    The table is designed to track ETL batch execution details, including:
    - Batch start and end timestamps.
    - Execution status (Running, Success, Failed).
    - ETL layer information (Bronze, Silver).
    - Error messages for failed batches.

Usage Notes:
    - Use this table to monitor and audit ETL batch runs.
    - Ensure proper indexing and maintenance for large-scale ETL operations.
===============================================================================
*/

-- ===========================================================
-- Drop and recreate etl.batch_control
-- ===========================================================
IF OBJECT_ID('etl.batch_control', 'U') IS NOT NULL
    DROP TABLE etl.batch_control;
GO

CREATE TABLE etl.batch_control (
    batch_id       BIGINT,
    batch_start    DATETIME,
    batch_end      DATETIME,
    batch_status   VARCHAR(20), -- Running / Success / Failed
    batch_layer    VARCHAR(20), -- Bronze / Silver
    error_message  VARCHAR(MAX)
);
