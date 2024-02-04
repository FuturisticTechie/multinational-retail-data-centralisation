
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

-- delete rows in order_table that don't correspond with rows 
-- in dim_products at the level of product_code column
DELETE FROM orders_table
WHERE NOT EXISTS (
    SELECT 1
    FROM dim_products
    WHERE dim_products.product_code = orders_table.product_code
);

-- Adding foreign keys in orders_table vs product_code
ALTER TABLE orders_table
ADD CONSTRAINT fk_products
FOREIGN KEY (product_code)
REFERENCES dim_products(product_code);