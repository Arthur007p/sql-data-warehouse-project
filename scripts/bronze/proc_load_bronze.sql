/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
This process loads data into the "Bronze" schema from external CSV files.
Performs the following actions:
- Truncates Bronze tables before loading the data.
- Uses the LOAD DATA LOCAL INFILE command to load data from CSV files into Bronze tables.

Parameters:
None.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

	SELECT '========================================================';
	SELECT 'Loading Bronze Layer';
	SELECT '========================================================';

	SELECT '--------------------------------------------------------';
	SELECT 'Loading CRM Table';
	SELECT '--------------------------------------------------------';

	SELECT '>> Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
    
    SELECT '>> Inserting Data Into: bronze.crim_cust_info';
	LOAD DATA LOCAL INFILE 'C:\\Users\\javi_\\Downloads\\sql-data-warehouse-project\\sql-data-warehouse-project\\datasets\\source_crm\\cust_info.csv' 
	INTO TABLE bronze.crm_cust_info 
	FIELDS TERMINATED BY ',' 
	ENCLOSED BY '"' 
	LINES TERMINATED BY '\n' 
	IGNORE 1 LINES;
	SELECT COUNT(*) FROM bronze.crm_cust_info;

	SELECT '>> Truncating Table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;
    
    SELECT '>> Inserting Data Into: bronze.crm_prd_info';
	LOAD DATA LOCAL INFILE 'C:\\Users\\javi_\\Downloads\\sql-data-warehouse-project\\sql-data-warehouse-project\\datasets\\source_crm\\prd_info.csv' 
	INTO TABLE bronze.crm_prd_info
	FIELDS TERMINATED BY ',' 
	ENCLOSED BY '"' 
	LINES TERMINATED BY '\r' 
	IGNORE 1 LINES;
	SELECT COUNT(*) FROM bronze.crm_prd_info;
    
	SELECT '>> Truncating Table: bronze.crm_sales_details ';
	TRUNCATE TABLE bronze.crm_sales_details; 
    
    SELECT '>> Inserting Data Into: bronze.crm_sales_details ';
	LOAD DATA LOCAL INFILE 'C:\\Users\\javi_\\Downloads\\sql-data-warehouse-project\\sql-data-warehouse-project\\datasets\\source_crm\\sales_details.csv' 
	INTO TABLE bronze.crm_sales_details 
	FIELDS TERMINATED BY ',' 
	ENCLOSED BY '"' 
	LINES TERMINATED BY '\n' 
	IGNORE 1 LINES;

	SELECT '--------------------------------------------------------';
	SELECT 'Loading ERP Table';
	SELECT '--------------------------------------------------------';

	SELECT '>> Truncating Table: bronze.erp_cust_az12 ';
	TRUNCATE TABLE bronze.erp_cust_az12; 
    
    SELECT '>> Inserting Data Into: bronze.erp_cust_az12';
	LOAD DATA LOCAL INFILE 'C:\\Users\\javi_\\Downloads\\sql-data-warehouse-project\\sql-data-warehouse-project\\datasets\\source_erp\\CUST_AZ12.csv' 
	INTO TABLE bronze.erp_cust_az12
	FIELDS TERMINATED BY ',' 
	ENCLOSED BY '"' 
	LINES TERMINATED BY '\n' 
	IGNORE 1 LINES;

	SELECT '>> Truncating Table: bronze.erp_loc_a101 ';
	TRUNCATE TABLE bronze.erp_loc_a101; 
    
    SELECT '>> Inserting Data Into: bronze.erp_loc_a101';
	LOAD DATA LOCAL INFILE 'C:\\Users\\javi_\\Downloads\\sql-data-warehouse-project\\sql-data-warehouse-project\\datasets\\source_erp\\LOC_A101.csv' 
	INTO TABLE bronze.erp_loc_a101
	FIELDS TERMINATED BY ',' 
	ENCLOSED BY '"' 
	LINES TERMINATED BY '\n' 
	IGNORE 1 LINES;

	SELECT '>> Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2; 
    
    SELECT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	LOAD DATA LOCAL INFILE 'C:\\Users\\javi_\\Downloads\\sql-data-warehouse-project\\sql-data-warehouse-project\\datasets\\source_erp\\PX_CAT_G1V2.csv' 
	INTO TABLE bronze.erp_px_cat_g1v2
	FIELDS TERMINATED BY ',' 
	ENCLOSED BY '"' 
	LINES TERMINATED BY '\n' 
	IGNORE 1 LINES;
