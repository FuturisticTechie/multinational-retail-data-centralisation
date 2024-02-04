

-- --first drop all rows where longtidue column is not a number
-- DELETE FROM dim_store_details
-- WHERE NOT longitude ~ E'^\\d+\\.?\\d*$';

-- Cast longitude and latitude column from TEXT to FLOAT
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT;


-- From dim_store_details, alter locality and continent from TEXT to VARCHAR(255)
ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN continent TYPE VARCHAR(255);

-- Find the maximum length of values in the store_code column
SELECT MAX(CHAR_LENGTH(CAST(store_code AS VARCHAR))) AS max_length
FROM dim_store_details;

-- Now use the output (11)to alter the data type of the store_code column
ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(11);

-- --first drop all rows where staff_nunmbers column is not a number
-- DELETE FROM dim_store_details
-- WHERE NOT staff_numbers ~ E'^\\d+\\.?\\d*$';

-- Alter staff_numbers from TEXT to SMALLINT
ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT;

-- Cast opening_date column from TEXT to DATE
ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE;

-- Alter store_type from TEXT to VARCHAR(255) NULLABLE
ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255) USING NULLIF(store_type, '')::VARCHAR(255);

-- Find the maximum length of values in the country_code column
SELECT MAX(CHAR_LENGTH(CAST(country_code AS VARCHAR))) AS max_length
FROM dim_store_details;

-- Now use the output (2)to alter the data type of the country_code column
ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2);