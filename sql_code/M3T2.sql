

--From dim_user_table, alter first_name and last_name from TEXT to VARCHAR(255)
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255);

-- Alter user_id from TEXT to uuid the data types
ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

-- Find the maximum length of values in the country_code column
SELECT MAX(CHAR_LENGTH(CAST(country_code AS VARCHAR))) AS max_length
FROM dim_users;

-- Now use the output (2)to alter the data type of the country_code column
ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(2);

-- Cast join_date column from TEXT to DATE
ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE USING join_date::DATE;