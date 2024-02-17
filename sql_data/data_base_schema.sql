-- Alter the data types
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

-- Alter the data type for product_quantity
ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT;

-- Find the maximum length of values in the card_number column
SELECT MAX(CHAR_LENGTH(CAST(card_number AS VARCHAR))) AS max_length
FROM orders_table;

-- Now use the output (19)to alter the data type of the card_number column
ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(19);

-- Find the maximum length of values in the store_code column
SELECT MAX(CHAR_LENGTH(CAST(store_code AS VARCHAR))) AS max_length
FROM orders_table;

-- Now use the output (12)to alter the data type of the store_code column
ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(12);

-- Find the maximum length of values in the product_code column
SELECT MAX(CHAR_LENGTH(CAST(product_code AS VARCHAR))) AS max_length
FROM orders_table;

-- Now use the output (11) to alter the data type of the product_code column
ALTER TABLE orders_table
ALTER COLUMN product_code TYPE VARCHAR(11);

------------------------------------------------


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

----------------------------------------------------





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

-- Now use the output (12)to alter the data type of the store_code column
ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(12);

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

--------------------------------------------------------

-- Remove £ character from the beginning of product_price column
UPDATE dim_products
SET product_price = TRIM('£' FROM product_price);

-- Add new column weight_class
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


--------------------------------------------------------



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


---------------------------------------------------------




-- Find the maximum length of values in the month column
SELECT MAX(CHAR_LENGTH(CAST(month AS VARCHAR))) AS max_length
FROM dim_date_times;

-- Now use the output (2)to alter the data type of the month column
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2);

-- Find the maximum length of values in the year column
SELECT MAX(CHAR_LENGTH(CAST(year AS VARCHAR))) AS max_length
FROM dim_date_times;

-- Now use the output (4)to alter the data type of the year column
ALTER TABLE dim_date_times
ALTER COLUMN year TYPE VARCHAR(4);

-- Find the maximum length of values in the day column
SELECT MAX(CHAR_LENGTH(CAST(day AS VARCHAR))) AS max_length
FROM dim_date_times;

-- Now use the output (2)to alter the data type of the day column
ALTER TABLE dim_date_times
ALTER COLUMN day TYPE VARCHAR(2);


-- Find the maximum length of values in the time_period column
SELECT MAX(CHAR_LENGTH(CAST(time_period AS VARCHAR))) AS max_length
FROM dim_date_times;

-- Now use the output (10)to alter the data type of the time_period column
ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10);

-- ALTER TABLE dim_date_times
ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

--------------------------------------------------------------


--Find the max len of values in card_number column
SELECT MAX(CHAR_LENGTH(CAST(card_number AS VARCHAR))) AS max_length
FROM dim_card_details;

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19);

--Now for expiry_date column
SELECT MAX(CHAR_LENGTH(CAST(expiry_date AS VARCHAR))) AS max_length
FROM dim_card_details;

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(10);

-- change type to date
ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;


---------------------------------------------------------------


-- Updtaing dim tables with primary keys matching orders_table
ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER Table dim_products
ADD PRIMARY KEY (product_code);


----------------------------------------------------------------


--Adding foreign keys in orders_table vs dim_date_times
ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_date_times
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times(date_uuid);


-- Adding foreign keys in orders_table vs dim_users
ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_users
FOREIGN KEY (user_uuid)
REFERENCES dim_users(user_uuid);


-- Adding foreign keys in orders_table vs card_number
ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_card_details
FOREIGN KEY (card_number)
REFERENCES dim_card_details(card_number);


-- Adding foreign keys in orders_table vs store_details
ALTER TABLE orders_table
ADD CONSTRAINT fk_dim_store_details
FOREIGN KEY (store_code)
REFERENCES dim_store_details(store_code);



-- Adding foreign keys in orders_table vs product_code
ALTER TABLE orders_table
ADD CONSTRAINT fk_products
FOREIGN KEY (product_code)
REFERENCES dim_products(product_code);




