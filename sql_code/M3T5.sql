

-- Rename removed column to still_available
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

-- Cast to float
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;

--Cast to DATE
ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE USING date_added::DATE;

--Cast to uuid
ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::UUID;


-- Update 'still_available' column to boolean values
--Note Still_avaliable is spelt incorrectly in the column
UPDATE dim_products
SET still_available = CASE 
                        WHEN still_available = 'Still_avaliable' THEN TRUE
                        WHEN still_available = 'Removed' THEN FALSE
                      END;
-- Cast to BOOL
ALTER TABLE dim_products
ALTER COLUMN still_available TYPE boolean USING still_available::boolean;


-- Find the maximum length of values in the EAN column
SELECT MAX(CHAR_LENGTH(CAST("EAN" AS VARCHAR))) AS max_length
FROM dim_products;

-- Now use the output (17)to alter the data type of the EAN column
ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(17);

-- Find the maximum length of values in the product_code column
SELECT MAX(CHAR_LENGTH(CAST(product_code AS VARCHAR))) AS max_length
FROM dim_products;

-- Now use the output (11)to alter the data type of the product_code column
ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(11);

-- Find the maximum length of values in the weight_class column
SELECT MAX(CHAR_LENGTH(CAST(weight_class AS VARCHAR))) AS max_length
FROM dim_products;

-- Now use the output (9)to alter the data type of the weight_class column
ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(9);