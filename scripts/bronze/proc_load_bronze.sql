/*
  =====================================================================
  Stored Procedure : Load Bronze Layer (Source -> Bronze)
  =====================================================================
  Script Purpose:
      This stored procedure loads data into the 'bronze' schema from external CSV files
      It performs the following actions:
      - Truncates the bronze table befrore loading data.
      - Uses the 'Bulk insert' command to load data from csv files to bronze table.

  parameters:
    None
  This stored procedure does not accept any parameters or return any values.

  Usage exemple:
    EXEC bronze.load_bronze;
*/

CREATE OR ALTER PROCEDURE BRONZE.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME;
	DECLARE @end_time DATETIME;
	DECLARE @total_start_time DATETIME;
	DECLARE @total_duration INT;
	
	BEGIN TRY
		SET @total_start_time = GETDATE();
		
		PRINT'=============================================================='
		PRINT'Loading Bronze Layer'
		PRINT'=============================================================='

		PRINT'--------------------------------------------------------------'
		PRINT'Loading CRM TABLES'
		PRINT'--------------------------------------------------------------'	

		SET @start_time = GETDATE();
		PRINT'>> TRUNCATING table: BRONZE.crm_cust_info'
		TRUNCATE TABLE BRONZE.crm_cust_info;
		PRINT'>> Inserting Data into : BRONZE.crm_cust_info'
		BULK INSERT BRONZE.crm_cust_info
		FROM 'C:\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
		PRINT'>> ----------------------'

		SET @start_time = GETDATE();
		PRINT'>> TRUNCATING table: BRONZE.crm_prd_info'
		TRUNCATE TABLE BRONZE.crm_prd_info;
		PRINT'>> Inserting Data into : BRONZE.crm_prd_info'
		BULK INSERT BRONZE.crm_prd_info
		FROM 'C:\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
		PRINT'>> ----------------------'

		SET @start_time = GETDATE();
		PRINT'>> TRUNCATING table: BRONZE.crm_sales_info'
		TRUNCATE TABLE BRONZE.crm_sales_info;
		PRINT'>> Inserting Data into : BRONZE.crm_sales_info'
		BULK INSERT BRONZE.crm_sales_info
		FROM 'C:\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
		PRINT'>> ----------------------'

		PRINT'--------------------------------------------------------------'
		PRINT'Loading ERP TABLES'
		PRINT'--------------------------------------------------------------'

		SET @start_time = GETDATE();
		PRINT'>> TRUNCATING table: BRONZE.erp_cust_info'
		TRUNCATE TABLE BRONZE.erp_cust_az12;
		PRINT'>> Inserting Data into : BRONZE.erp_cust_info'
		BULK INSERT BRONZE.erp_cust_az12
		FROM 'C:\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
		PRINT'>> ----------------------'

		SET @start_time = GETDATE();
		PRINT'>> TRUNCATING table: BRONZE.erp_loc_a101'
		TRUNCATE TABLE BRONZE.erp_loc_a101;
		PRINT'>> Inserting Data into : BRONZE.erp_loc_a101'
		BULK INSERT BRONZE.erp_loc_a101
		FROM 'C:\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
		PRINT'>> ----------------------'

		SET @start_time = GETDATE();
		PRINT'>> TRUNCATING table: BRONZE.erp_px_cat_g1v2'
		TRUNCATE TABLE BRONZE.erp_px_cat_g1v2;
		PRINT'>> Inserting Data into : BRONZE.erp_px_cat_g1v2'
		BULK INSERT BRONZE.erp_px_cat_g1v2
		FROM 'C:\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration : ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' seconds';
		PRINT'>> ----------------------'
		
		SET @total_duration = DATEDIFF(second, @total_start_time, GETDATE());
		PRINT'=============================================================='
		PRINT'Bronze Layer Loading Completed Successfully'
		PRINT'Total Duration : ' + CAST(@total_duration AS NVARCHAR) + ' seconds';
		PRINT'=============================================================='
		
	END TRY

	BEGIN CATCH
		PRINT'=============================================================='
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT'Error message: ' + ERROR_MESSAGE();
		PRINT'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'=============================================================='
	END CATCH
END
