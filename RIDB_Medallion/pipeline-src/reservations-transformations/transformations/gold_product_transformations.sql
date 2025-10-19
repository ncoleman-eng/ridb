-- Step 1: Create the target streaming table
USE CATALOG ${catalog_prefix}ridb;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW dim_product
AS 
SELECT DISTINCT
      product_sk,
      product_id,
      inventory_type,
      use_type,
      site_type,          
      current_timestamp as updated_at           
    FROM ${catalog_prefix}ridb.silver.reservations
    WHERE product_id IS NOT NULL