-- Change [Path] placeholder and add paths of CSV files
USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

	TRUNCATE TABLE bronze.crm_cust_info;
	
	BULK INSERT bronze.crm_cust_info
	FROM '[Path]\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	)

	SELECT COUNT(*) crm_cust_info FROM bronze.crm_cust_info;

	TRUNCATE TABLE bronze.crm_prd_info;
	
	BULK INSERT bronze.crm_prd_info
	FROM '[Path]\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	)
	
	SELECT COUNT(*) crm_prd_info FROM bronze.crm_prd_info;
	

	TRUNCATE TABLE bronze.crm_sales_details;
	
	BULK INSERT bronze.crm_sales_details
	FROM '[Path]\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	)
	
	SELECT COUNT(*) crm_sales_details FROM bronze.crm_sales_details;

	

	TRUNCATE TABLE bronze.erp_cust_az12;
	
	BULK INSERT bronze.erp_cust_az12
	FROM '[Path]\CUST_AZ12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	)
	
	SELECT COUNT(*) erp_cust_az12 FROM bronze.erp_cust_az12;
	

	TRUNCATE TABLE bronze.erp_loc_a101;
	
	BULK INSERT bronze.erp_loc_a101
	FROM '[Path]\LOC_A101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	)
	
	SELECT COUNT(*) erp_loc_a101 FROM bronze.erp_loc_a101;
	

	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM '[Path]\PX_CAT_G1V2.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	)
	
	SELECT COUNT(*)  px_cat_g1v2 FROM bronze.erp_px_cat_g1v2;
	
END
