/*
==================================================
Stored Procedure: Load Bronze Layer
==================================================
Script purpose:
  Stored procedure to insert bulk data into the bronze tables. First it will truncate the tables and insert the data into the tables from CSV files.

Parameters:
  No Parameters. The SP will also not return any value.

Usage:
  EXEC bronze_load_bronze.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME,@B_start_time DATETIME, @B_end_time DATETIME;

	SET @B_start_time = getdate();

	BEGIN TRY
	
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==========================================';

		PRINT '------------------------------------------';
		PRINT 'Loading CRM Table'
		PRINT '------------------------------------------';
	
		SET @start_time = getdate();
 
		PRINT '>>Truncating the table : crm_cust_info';	

		TRUNCATE TABLE bronze.crm_cust_info;
	
		PRINT 'Inserting data into : crm_cust_info';

		BULK INSERT bronze.crm_cust_info
		FROM 'D:\pavan\Data Analyst\SQL\Baara\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = getdate();

		PRINT 'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' second' ;

		PRINT '--------------';

		SET @start_time = getdate();

		PRINT '>>Truncating the table : crm_prd_info';

		TRUNCATE TABLE bronze.crm_prd_info;
	
		PRINT 'Inserting data into : crm_prd_info';
	
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\pavan\Data Analyst\SQL\Baara\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = getdate();

		PRINT 'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' second' ;

		PRINT '--------------';

		SET @start_time = getdate();
	
		PRINT '>>Truncating the table : crm_sales_details';
	
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT 'Inserting data into : crm_sales_details';	

		BULK INSERT bronze.crm_sales_details
		FROM 'D:\pavan\Data Analyst\SQL\Baara\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = getdate();

		PRINT 'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' second' ;

		PRINT '------------------------------------------';
		PRINT 'Loading ERP Table'
		PRINT '------------------------------------------';

		SET @start_time = getdate();
	
		PRINT '>>Truncating the table : erp_cust_az12';

		TRUNCATE TABLE bronze.erp_cust_az12;
	
		PRINT 'Inserting data into : erp_cust_az12';
	
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\pavan\Data Analyst\SQL\Baara\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


		SET @end_time = getdate();

		PRINT 'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' second' ;

		PRINT '--------------';

		SET @start_time = getdate();
	
		PRINT '>>Truncating the table : erp_loc_a101';

		TRUNCATE TABLE bronze.erp_loc_a101;
	
		PRINT 'Inserting data into : erp_loc_a101';	

		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\pavan\Data Analyst\SQL\Baara\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);


		SET @end_time = getdate();

		PRINT 'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' second' ;

		PRINT '--------------';

		SET @start_time = getdate();
	
		PRINT '>>Truncating the table : erp_px_cat_g1v2';	

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT 'Inserting data into : erp_px_cat_g1v2';

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\pavan\Data Analyst\SQL\Baara\Projects\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = getdate();

		PRINT 'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' second' ;


	END TRY

	BEGIN CATCH
		PRINT '=======================================';
		PRINT 'Error occured during loading the bronze layer';
		PRINT 'Error message: '+error_message();
		PRINT 'Error state: '+CAST(ERROR_STATE() AS NVARCHAR(100))
		PRINT '=======================================';
	END CATCH

	PRINT '-----------';

	SET @B_end_time = getdate();

	PRINT 'Load duration of whole bronze layer: '+CAST(DATEDIFF(second,@B_start_time,@B_end_time) AS NVARCHAR(20)) + ' second' ;

END
