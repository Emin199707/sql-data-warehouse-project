-- Firth we will check if there is difference between cst_key and cid in bronze.erp_loc_a101 and silver.crm_cust_info tables:
SELECT 
cid,
cntry
FROM bronze.erp_loc_a101;
GO
SELECT cst_key FROM silver.crm_cust_info;
-- And then we will transoform data, also check it at the same time:
SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN
(SELECT cst_key FROM silver.crm_cust_info); -- No result expected.

--CHecking distinct country types:
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;

-- Inserting into silver layer erp customer location table:
INSERT INTO silver.erp_loc_a101
(cid, cntry)
SELECT 
REPLACE(cid, '-', '') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

-- Quality check
SELECT DISTINCT cntry
FROM .erp_loc_a101silver
ORDER BY cntry;
GO
SELECT FROM silver.erp_loc_a101;


