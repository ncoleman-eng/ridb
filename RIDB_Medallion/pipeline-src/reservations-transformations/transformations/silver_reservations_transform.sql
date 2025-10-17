-- Step 1: Create the target streaming table
CREATE OR REFRESH STREAMING TABLE reservations;

-- Step 2: Use Auto CDC for SCD Type 1 (default behavior)
CREATE FLOW reservations_auto_cdc AS
  AUTO CDC INTO reservations
  FROM (
    -- Step 3: Apply cleansing/standardization transformations
    SELECT
      order_number AS reservation_id,
      historical_reservation_id,
      to_timestamp(order_date) AS reservation_timestamp,
      to_date(order_date) AS reservation_date,
      COALESCE(DATEDIFF(end_date, start_date), 0) AS calculated_reservation_days,
      to_date(start_date) AS reservation_start_date,
      to_date(end_date) AS reservation_end_date,
      CAST(number_of_people AS INT) AS number_of_people,
      CASE
        WHEN REGEXP_REPLACE(TRIM(customer_zip), '[^0-9]', '') RLIKE '^[0-9]$'
             AND REGEXP_REPLACE(TRIM(customer_zip), '[^0-9]', '') <> '00000'
          THEN REGEXP_REPLACE(TRIM(customer_zip), '[^0-9]', '')
        WHEN REGEXP_REPLACE(TRIM(customer_zip), '[^0-9]', '') RLIKE '^[0-9]{5,}$'
          THEN SUBSTRING(REGEXP_REPLACE(TRIM(customer_zip), '[^0-9]', ''), 1, 5)
        ELSE NULL
      END AS customer_zip,
      org_id,
      agency,
      parent_location_id,
      UPPER(parent_location) AS parent_location_name,
      facility_id,
      UPPER(park) AS park_name,
      NULLIF(legacy_facility_id,'') AS legacy_facility_id,     
      CAST(facility_latitude AS DECIMAL(8,6)) AS facility_latitude,
      CAST(facility_longitude AS DECIMAL(9,6)) AS facility_longitude,
      NULLIF(UPPER(facility_state),'') AS facility_state,
      CASE
        WHEN REGEXP_REPLACE(TRIM(facility_zip), '[^0-9]', '') RLIKE '^[0-9]$'
             AND REGEXP_REPLACE(TRIM(facility_zip), '[^0-9]', '') <> '00000'
          THEN REGEXP_REPLACE(TRIM(facility_zip), '[^0-9]', '')
        WHEN REGEXP_REPLACE(TRIM(facility_zip), '[^0-9]', '') RLIKE '^[0-9]{5,}$'
          THEN SUBSTRING(REGEXP_REPLACE(TRIM(facility_zip), '[^0-9]', ''), 1, 5)
        ELSE NULL
      END AS facility_zip,
      region_code,
      UPPER(region_description) AS region_description,
      product_id,
      UPPER(inventory_type) AS inventory_type,
      UPPER(use_type) AS use_type,
      UPPER(site_type) AS site_type,
      CASE
        WHEN equipment_description = ''
        OR equipment_length = ''
        THEN NULL
        ELSE SHA2(concat_ws('|',COALESCE(NULLIF(UPPER(TRIM(equipment_description)),''),''),COALESCE(NULLIF(UPPER(TRIM(equipment_length)),''),'')), 256)
      END AS equipment_durable_key,
      COALESCE(NULLIF(UPPER(TRIM(equipment_description)),''),'') AS equipment_description,
      COALESCE(NULLIF(UPPER(TRIM(equipment_length)),''),'') AS equipment_length,      
      CAST(attr_fee AS DECIMAL) AS attr_fee,
      CAST(discount AS DECIMAL) AS discount,
      CAST(tax AS DECIMAL) AS tax,
      CAST(total_before_tax AS DECIMAL) AS total_before_tax,
      CAST(total_paid AS DECIMAL) AS total_paid,
      CAST(tran_fee AS DECIMAL) AS tran_fee,
      CAST(use_fee AS DECIMAL) AS use_fee,
      ingested_at,
      current_timestamp AS updated_at
    FROM STREAM(dev_ridb.bronze.reservations) 
)
  KEYS (historical_reservation_id)
  SEQUENCE BY (ingested_at, reservation_timestamp, calculated_reservation_days)
  COLUMNS * EXCEPT(ingested_at);