-- Check for NULL or Duplicated product IDs in bronze schema crm_prd_info table
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 or prd_id IS NULL;

-- CHheck bronze.erp_px_cat_g1v2 table's unique IDs and formats which example is CO_BR and add <<REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id>> to our overall query
SELECT DISTINCT id from bronze.erp_px_cat_g1v2;

-- Checking Negative or NULL costs in bronze schema and add <<ISNULL(prd_cost, 0) AS prd_cost>> to our query
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

/* Checking Distinct pro_lines for adding more obvious information abour prd_lines and adding below code for our overall query
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 ELSE 'n/a'
*/

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;
  
-- Checking if our bronze schema tables have data row where start date is later than end date.
SELECT * FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Then testing our Transofrmation with some examples
SELECT prd_id,
	   prd_key,
	   prd_nm,
	   prd_start_dt,
	   prd_end_dt,
	   LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- If the result is expected then we will add LEAD method to our overall Insert query

-- And finally as genereally done in the silver layer, we will decide what to add or change in Silver schema table structure. Our initial structure was like this:
IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
); -- Do no not run if you have this table already

-- And after data cleansing and transformation , and before inserting we will change the strcuture like this:

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
  cat_id  NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Finally we will insert data to the silver layer table:

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
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	CAST (prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

-- Quality check scripts for silver layer table after Inserting:
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- ------------------------------------

SELECt prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- ------------------------------------


SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- ------------------------------------


SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- -----------------------------------


SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;
