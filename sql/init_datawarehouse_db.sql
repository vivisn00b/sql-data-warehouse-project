/*
===============================================================================
  Create Database and Schemas
===============================================================================
  Script Purpose:
      - Creates a new database named 'DataWarehouse'.
      - If the database already exists, it is dropped and recreated.
      - Creates three schemas within the database:
            • bronze
            • silver
            • gold

  WARNING:
      Running this script will drop the entire 'DataWarehouse' database
      if it exists. ALL DATA WILL BE PERMANENTLY DELETED.

      Ensure proper backups exist before executing.

-------------------------------------------------------------------------------
  Learning Notes:
  
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

      • SET SINGLE_USER
            Restricts database access to only one active connection.

      • WITH ROLLBACK IMMEDIATE
            - Terminates all other active sessions immediately.
            - Rolls back any open transactions.
            - Prevents waiting for users to disconnect gracefully.

      Why this is needed:
            SQL Server will not allow a database to be dropped
            while it is in use by other connections.

      Important:
            - All active transactions are forcibly rolled back.
            - All other connected sessions are terminated instantly.
            - The session executing this command becomes the only user.
===============================================================================
*/

USE master;
GO

/*=============================================================================
  Drop Existing Database (If Exists)
=============================================================================*/
IF EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = N'DataWarehouse'
)
BEGIN
    PRINT 'Database exists. Forcing SINGLE_USER mode and dropping...';

    ALTER DATABASE DataWarehouse
        SET SINGLE_USER
        WITH ROLLBACK IMMEDIATE;

    DROP DATABASE DataWarehouse;

    PRINT 'Database dropped successfully.';
END
ELSE
BEGIN
    PRINT 'Database does not exist. Creating new database...';
END;
GO


/*=============================================================================
  Create Database
=============================================================================*/
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO


/*=============================================================================
  Create Schemas
=============================================================================*/
PRINT 'Creating schemas...';
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

PRINT 'Database and schemas created successfully.';
GO
