

-- Remove £ character from the beginning of product_price column
UPDATE dim_products
SET product_price = TRIM('£' FROM product_price);

Add new column weight_class
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(255);

-- Update weight_class based on weight range
UPDATE dim_products
SET weight_class =
  CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
  END;

--Find out all rows where null has now been introduced 
SELECT *
FROM dim_products
WHERE date_added IS NULL OR uuid IS NULL or removed IS NULL or product_code is NULL OR weight_class is NULL;

--Drop all rows with null values
DELETE FROM dim_products
WHERE index IN (788, 1217, 1841, 1133, 1400, 794, 307, 751);