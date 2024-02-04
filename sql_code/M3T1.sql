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





