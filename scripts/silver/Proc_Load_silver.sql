CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME, @silver_start_time DATETIME, @silver_end_time DATETIME;

	SET @silver_start_time = getdate();

	BEGIN TRY

		PRINT '==========================================';
		PRINT 'Loading Silver Layer';
		PRINT '==========================================';

		PRINT '------------------------------------------';
		PRINT 'Loading CRM Table'
		PRINT '------------------------------------------';

		SET @start_time = getdate();

		PRINT '>> Truncating table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting data into: silver.crm_cust_info';

		INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firstname,cst_lastname,cst_martial_status,cst_gndr,cst_create_date)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE	
				WHEN UPPER(TRIM(cst_martial_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_martial_status)) = 'S' THEN 'Single'
				ELSE 'n/a'
			END cst_martial_status, -- Normalize martial status values to readable format
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'n\a'
			END cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM(
			SELECT 
					*,
					ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) rank
				FROM bronze.crm_cust_info
				WHERE cst_id IS NOT NULL)t
		WHERE rank = 1 -- Select the most recent record per customer

		SET @end_time = getdate();

		PRINT 'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' second' ;

		PRINT '--------------';

		SET @start_time = getdate();

		PRINT '>> Truncating table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting data into: silver.crm_prd_info';

		INSERT INTO silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
			SELECT
				prd_id,
				REPLACE(SUBSTRING(prd_key,1,5),'-','_') cat_id,
				SUBSTRING(prd_key,7,LEN(prd_key)) prd_key,
				prd_nm,
				ISNULL(prd_cost,0) prd_cost,
				CASE UPPER(TRIM(prd_line)) 
					WHEN 'M' THEN 'Mountain'
					WHEN 'R' THEN 'Road'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'n/a'
				END prd_line,
				CAST(prd_start_dt AS DATE) prd_start_dt,
				CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) prd_end_dt
			FROM bronze.crm_prd_info

		SET @end_time = getdate();

		PRINT 'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' second' ;

		PRINT '--------------';

		SET @start_time = getdate();

		PRINT '>> Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting data into: silver.crm_sales_details';

		INSERT INTO silver.crm_sales_details(sls_order_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales,sls_quantity,sls_price)
		SELECT
			sls_order_num,
			sls_prd_key,
			sls_cust_id,
			CASE	
				WHEN LEN(sls_order_dt) <> 8 OR sls_order_dt = 0 THEN NULL
				ELSE FORMAT(CAST(CAST(sls_order_dt AS VARCHAR) AS DATE),'yyyy-MM-dd')
			END AS sls_order_dt,
			CASE	
				WHEN LEN(sls_order_dt) <> 8 OR sls_order_dt = 0 THEN NULL
				ELSE FORMAT(CAST(CAST(sls_order_dt AS VARCHAR) AS DATE),'yyyy-MM-dd')
			END AS sls_ship_dt,
			CASE	
				WHEN LEN(sls_order_dt) <> 8 OR sls_order_dt = 0 THEN NULL
				ELSE FORMAT(CAST(CAST(sls_order_dt AS VARCHAR) AS DATE),'yyyy-MM-dd')
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE	
				WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / sls_quantity
				ELSE sls_price
			END AS sls_price -- Derive price if original price is invalid
		FROM bronze.crm_sales_details;

		SET @end_time = getdate();

		PRINT 'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' second' ;

		PRINT '--------------';

		PRINT '------------------------------------------';
		PRINT 'Loading CRM Table'
		PRINT '------------------------------------------';

		SET @start_time = getdate();

		PRINT '>> Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting data into: silver.erp_cust_az12';

		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
		SELECT 
			CAST(RIGHT(cid,5) AS INT) cid,
			CASE 
				WHEN bdate > getdate() THEN NULL
				ELSE bdate
			END AS bdate,
			CASE
				WHEN TRIM(gen) = 'F' THEN 'Female'
				WHEN TRIM(gen) = 'M' THEN 'Male'
				WHEN gen IS NULL OR gen = ' ' THEN 'n/a'
				ELSE TRIM(gen)
			END gen
		FROM bronze.erp_cust_az12;

		SET @end_time = getdate();

		PRINT 'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' second' ;

		PRINT '--------------';

		SET @start_time = getdate();

		PRINT '>> Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting data into: silver.erp_loc_a101';

		INSERT INTO silver.erp_loc_a101(cid,cntry)
		SELECT
			CAST(RIGHT(cid,5) AS INT) cid,
				CASE	
					WHEN TRIM(cntry) = 'DE' THEN 'Germany'
					WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
					WHEN cntry IS NULL OR cntry = ' ' THEN 'n/a'
					ELSE cntry
				END AS cntry
			FROM bronze.erp_loc_a101;

		SET @end_time = getdate();

		PRINT 'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' second' ;

		PRINT '--------------';

		SET @start_time = getdate();

		PRINT '>> Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting data into: silver.erp_px_cat_g1v2';

		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenanace)
		SELECT
			id,
			cat,
			subcat,
			maintenanace	
		FROM bronze.erp_px_cat_g1v2;

		SET @end_time = getdate();

		PRINT 'Load duration: '+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(20)) + ' second' ;

		PRINT '--------------';

		SET @silver_end_time = getdate();

		PRINT 'Load duration: '+CAST(DATEDIFF(second,@silver_start_time,@silver_end_time) AS NVARCHAR(20)) + ' second' ;

	END TRY
	
	BEGIN CATCH
		PRINT 'Error inserting the data into silver tables from bronze tables';
		PRINT 'Error message: '+error_message();
	END CATCH

END

exec silver.load_silver
