-- Firstly we will check if there is unmatching cid between bronze.erp_cust_az12 and silver.crm_cust_info:
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);  -- No result expected after transofrmation.

-- Checking Customers birthdates, who is older than 100 years or whos birthday is greater than today:
SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

--Check distinct gender types in bronze table:
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

-- Inserting into silver erp customer table:
INSERT INTO silver.erp_cust_az12(cid, bdate,gen)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

--Qualit checks:
SELECT DISTINCT gen
FROM silver.erp_cust_az12;
GO
SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE(); -- Older birthdates will be in the output, because we only changed birthdates whinch is greater than today.



