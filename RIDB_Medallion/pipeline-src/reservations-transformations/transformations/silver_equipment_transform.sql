-- Step 1: Create the target streaming table
CREATE OR REFRESH MATERIALIZED VIEW equipment
AS 
SELECT DISTINCT
      equipment_durable_key,
      equipment_description,
      equipment_length,          
      current_timestamp as created_at           
    FROM dev_ridb.silver.reservations
    WHERE equipment_durable_key IS  NULL
