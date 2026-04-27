/*
==============================================================
Data cleansing and transformation for each table in Silver schema consist of 3 steps.
==============================================================
*/

/*
==============================================================
1) We will first check bronze schema tables for data consistency.
==============================================================
*/

--Check For Nulls or Duplicates in Primary Key
SELECT cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id IS NULL

--Check for unwanted Spaces
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
-- and for cst_lastname
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Data Standization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;
-- and for cst_marital_status
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;
/*
==============================================================
2) Insert into silver schema tables cleaned and transformed data.
==============================================================
*/

INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;

/*
==============================================================
3) Check silver schema tables for data consistency.
==============================================================
*/

--Check For Nulls or Duplicates in Primary Key  
--No result expected
SELECT cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id IS NULL

--Check for unwanted Spaces in cst_firstname 
--No result expected
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
-- and for cst_lastname 
--No result expected
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Data Standization & Consistency, expected results ( Male, Female, n/a )
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;
-- and for cst_marital_status, expected results ( Single, Married )
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- And final select for all data
SELECT * FROM silver.crm_cust_info;

