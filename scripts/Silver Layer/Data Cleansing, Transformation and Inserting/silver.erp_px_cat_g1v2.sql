-- Checking unwanted spaces in the columns:
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Checking data standartization:
SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2;
GO
SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2;
GO
SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2;

--Inserting Into silver tables

INSERT INTO silver.erp_px_cat_g1v2
(id, cat, subcat, maintenance)
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;
