

-- Find the maximum length of values in the month column
SELECT MAX(CHAR_LENGTH(CAST(month AS VARCHAR))) AS max_length
FROM dim_date_times;

-- Now use the output (10)to alter the data type of the month column
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(10);

-- Find the maximum length of values in the year column
SELECT MAX(CHAR_LENGTH(CAST(year AS VARCHAR))) AS max_length
FROM dim_date_times;

-- Now use the output (10)to alter the data type of the year column
ALTER TABLE dim_date_times
ALTER COLUMN year TYPE VARCHAR(10);

-- Find the maximum length of values in the day column
SELECT MAX(CHAR_LENGTH(CAST(day AS VARCHAR))) AS max_length
FROM dim_date_times;

-- Now use the output (10)to alter the data type of the day column
ALTER TABLE dim_date_times
ALTER COLUMN day TYPE VARCHAR(10);


-- Find the maximum length of values in the time_period column
SELECT MAX(CHAR_LENGTH(CAST(time_period AS VARCHAR))) AS max_length
FROM dim_date_times;

-- Now use the output (10)to alter the data type of the time_period column
ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10);

-- ALTER TABLE dim_date_times
ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

