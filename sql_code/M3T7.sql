

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