/*
=====================================================
CREATE DATABASE and SCHEMAS
=====================================================
Script Purpose:
  The purpose of the Script is to create Database with name DataWarehouse if database is alreadey exists it will drop that database and create a new one with DataWarehouse name.1
Additionally the script creates 3 schemas bronze, silver and gold.

Warning:
  Using this script may drop the database if already exists in your SQL Server. Check if there is any database name with DataWarehouse before running this script
or you may loose all your data in that Database.

*/

USE master;

GO

--Drop and recreate the 'Datawarehouse' Datasbase
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	DROP DATABASE DataWarehouse
END;
 
GO

-- Create database DataWarehouse

CREATE DATABASE DateWarehouse;

GO

USE DataWarehouse;
GO

--Create schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
