/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL Silver.load_silver;
===============================================================================
*/

DELIMITER //
DROP PROCEDURE IF EXISTS silver.load_silver;
CREATE PROCEDURE silver.load_silver()
BEGIN
	SELECT '>>Truncating Table: silver.crm_cust_info' AS message_1;
	TRUNCATE TABLE silver.crm_cust_info;
	SELECT '>>Truncating Data Into: silver.crm_cust_info' AS message_2;
	INSERT INTO silver.crm_cust_info (
		cst_id, 
		cst_key, 
		cst_firstname, 
		cst_lastname, 
		cst_material_status, 
		cst_gndr, 
		cst_create_date)
	SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_material_status, -- Normalize marital status values to readable format
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END AS cstgndr, -- Normalize gender values to readable format
		cst_create_date
	FROM (
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
	) AS t 
	WHERE flag_last =1; -- Select the mos recent record per customer
    
SELECT '>>Truncating Table: silver.crm_prd_info' AS message_3;
TRUNCATE TABLE silver.crm_prd_info;
SELECT '>>Truncating Data Into: silver.crm_prd_info' AS message_4;   
    
    INSERT INTO silver.crm_prd_info (
prd_id, 
cat_id,
prd_key, 
prd_nm, 
prd_cost, 
prd_line, 
prd_start_dt, 
prd_end_dt
)
SELECT
	prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    prd_nm,
    IFNULL(prd_cost, 0) AS prd_cost,
    CASE  UPPER(TRIM(prd_line)) 
		WHEN  'R' THEN 'Mountain' 
        WHEN  'S' THEN 'Road'
        WHEN  'M' THEN 'Other Sales' 
        WHEN  'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    prd_start_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY   prd_start_dt) AS prd_end_dt
FROM bronze.crm_prd_info;

	SELECT '>>Truncating Table: silver.crm_sales_details' AS message_5;
	TRUNCATE TABLE silver.crm_sales_details;
	SELECT '>>Truncating Data Into: silver.crm_sales_details' AS message_6;

INSERT INTO silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
SELECT
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN null
        ELSE CAST(sls_order_dt AS DATE)
    END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN null
        ELSE CAST(sls_ship_dt AS DATE)
    END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN null
        ELSE CAST(sls_due_dt AS DATE)
    END AS sls_due_dt,
   CASE
		WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
    sls_quantity,
    CASE
		WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price -- Derive price if original value is invalid
  END AS sls_price
  FROM bronze.crm_sales_details;
  
  	SELECT '>>Truncating Table: silver.erp_cust_az12' AS message_7;
  	TRUNCATE TABLE silver.erp_cust_az12;
  	SELECT '>>Truncating Data Into: silver.erp_cust_az12' AS message_8;
      
  INSERT INTO silver.erp_cust_az12 (
  cid,
  bdate,
  gen)
  SELECT
      CASE
  		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LENGTH(cid))
          ELSE cid -- Remove 'NAS' prefix if present
      END AS cud,
      CASE
  		WHEN BDATE > NOW() THEN NULL
          ELSE bdate
  	END AS bdate, -- Set furure birhdates ro NULL
       CASE
  		WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('F', 'FEMALE') THEN 'Female'
          WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('M', 'MALE') THEN 'Male'
          ELSE 'n/a'
      END AS gen
  FROM bronze.erp_cust_az12;
  
  	SELECT '>>Truncating Table: silver.erp_Loc_a101' AS message_9;
  	TRUNCATE TABLE silver.erp_Loc_a101;
  	SELECT '>>Truncating Data Into: silver.erp_Loc_a101' AS message_10;
  
  INSERT INTO silver.erp_Loc_a101 (
  	cid,
      cntry
  )
  SELECT
  	REPLACE(cid, '-', '') AS cid,
          CASE
  		WHEN UPPER(TRIM(REPLACE(REPLACE(cntry, '\r', ''), '\n', ''))) = 'DE' THEN 'Germany'
          WHEN UPPER(TRIM(REPLACE(REPLACE(cntry, '\r', ''), '\n', ''))) IN ('US', 'USA') THEN 'United States'
          WHEN TRIM(REPLACE(REPLACE(cntry, '\r', ''), '\n', '')) = '' OR cntry IS NULL THEN 'n/a'
          ELSE TRIM(cntry)
      END AS cntry
  FROM bronze.erp_loc_a101;
  
  	SELECT '>>Truncating Table: silver.erp_px_cat_g1v2' AS message_11;
  	TRUNCATE TABLE silver.erp_px_cat_g1v2;
  	SELECT '>>Truncating Data Into: silver.erp_px_cat_g1v2' AS message_12;
  
  TRUNCATE TABLE silver.erp_px_cat_g1v2;
  INSERT INTO silver.erp_px_cat_g1v2
  (
  	id,
  	cat,
  	subcat,
  	maintenange
  )
  SELECT
  	id,
      cat,
      subcat,
  	CASE
  		WHEN UPPER(TRIM(REPLACE(REPLACE(maintenange, '\r', ''), '\n', ''))) = 'YES' THEN 'Yes'
  		ELSE maintenange
      END AS maintenage
  FROM bronze.erp_px_cat_g1v2;
END //
