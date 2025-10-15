-- Step 2: Create the target streaming table
CREATE OR REFRESH STREAMING TABLE reservations;

-- Step 3: Use APPLY CHANGES INTO for SCD Type 1 (default behavior)
CREATE FLOW reservations_auto_cdc AS
  AUTO CDC INTO reservations
  FROM (
    SELECT
      CAST(order_number AS STRING)       AS reservation_id,
      historical_reservation_id,
      CAST(order_date AS DATE)           AS reservation_date,
      CAST(start_date AS DATE)           AS start_date,
      CAST(end_date AS DATE)             AS end_date,
      COALESCE(customer_zip, 'Unknown')  AS customer_zip,
      nights                             AS number_of_days,
      COALESCE(DATEDIFF(end_date, start_date), 0) AS calculated_days,
      CAST(number_of_people AS INT)      AS number_of_people,
      org_id                             AS agency_id,
      agency,            
      parent_location_id,
      parent_location,
      facility_id,
      park                               AS facility_name,
      legacy_facility_id,
      facility_latitude,
      facility_longitude,
      facility_state,
      facility_zip,
      region_code,
      region_description,
      product_id,
      site_type,
      equipment_description,
      equipment_length,
      inventory_type,
      use_type,
      CAST(attr_fee AS INT)             AS attr_fee,
      CAST(discount AS INT)              AS discount,
      CAST(tax AS INT)                   AS tax,
      CAST(total_before_tax AS INT)      AS total_before_tax,
      CAST(total_paid AS INT)            AS total_paid,
      CAST(tran_fee AS INT)              AS tran_fee,
      CAST(use_fee AS INT)               AS use_fee,        
      ingested_at
      
      
    FROM STREAM(dev_ridb.bronze.reservations)
  )
  KEYS (historical_reservation_id)
  SEQUENCE BY (ingested_at, reservation_date, calculated_days)
  COLUMNS * EXCEPT(ingested_at);