
-- Updtaing dim tables with primary keys matching orders_table
ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

DELETE FROM dim_date_times WHERE date_uuid IS NULL;

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

DELETE FROM dim_store_details WHERE store_code IS NULL;

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER Table dim_products
ADD PRIMARY KEY (product_code);