
-- Task 1
-- Total numnber of stores per country
SELECT country_code AS country,
	COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country
ORDER BY total_no_stores DESC;



-- Task 2
-- locations with most stores
SELECT locality AS locality,
	COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;


-- Task 3
-- Get the total sales in each month, from highest to lowest
-- Only return the highest 6
SELECT ROUND(CAST(SUM(orders.product_quantity * products.product_price) AS numeric),2) AS total_sales,
    dates.month AS "month"
FROM orders_table AS orders
    JOIN dim_date_times AS dates ON orders.date_uuid = dates.date_uuid
    JOIN dim_products AS products ON orders.product_code = products.product_code
GROUP BY dates.month
ORDER BY total_sales DESC
LIMIT 6;


--Task 4
--Online vs Offline sales
SELECT 
	COUNT (orders_table.product_quantity) AS number_of_sales,
	SUM (orders_table.product_quantity) AS product_quantity_count,
	CASE 
		WHEN dim_store_details.store_code = 'WEB-1388012W' THEN 'Web'
		ELSE 'Offline'
		END AS location
FROM orders_table
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN dim_products ON orders_table.product_code = dim_products.product_code

GROUP BY location
ORDER BY number_of_sales ASC;

-- Task 5
-- Percentage sales comingb through each type of store

SELECT
    dim_store_details.store_type AS store_type,
    ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric, 2) AS total_sales,
    ROUND((SUM(100.0 * orders_table.product_quantity * dim_products.product_price) / 
           SUM(SUM(orders_table.product_quantity * dim_products.product_price)) OVER ())::numeric, 2) AS percentage_total
FROM
    orders_table
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN
    dim_products ON dim_products.product_code = orders_table.product_code
JOIN
    dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
GROUP BY
    store_type
ORDER BY
    percentage_total DESC;

--Task 6
--which month in the year produced the highest cost of sales
SELECT
	ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric, 2) AS total_sales,
	dim_date_times.year AS year,
	dim_date_times.month AS month
	
FROM orders_table
JOIN dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
JOIN dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY year, month
ORDER BY total_sales DESC
LIMIT 10;

--Task 7
-- Staff headcount

SELECT
	SUM(staff_numbers) AS total_staff_numbers,
	country_code

FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

-- Task 8
--which German store type is selling the most

SELECT
	ROUND(SUM(orders_table.product_quantity * dim_products.product_price)::numeric, 2) AS total_sales,
	dim_store_details.store_type,
	dim_store_details.country_code

FROM orders_table
JOIN dim_products ON dim_products.product_code = orders_table.product_code
JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY dim_store_details.store_type, dim_store_details.country_code
ORDER BY total_sales ASC;

-- Task 9
-- How quickly is the company making sales
-- the average time taken between each sale grouped by year

WITH cte AS (
    SELECT 
        TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS') AS datetimes, 
        year 
    FROM 
        dim_date_times
    ORDER BY 
        datetimes DESC
), cte2 AS (
    SELECT 
        year, 
        datetimes, 
        LEAD(datetimes, 1) OVER (ORDER BY datetimes DESC) AS time_difference 
    FROM 
        cte
) 
SELECT 
    year, 
    JSON_BUILD_OBJECT(
        'hours', EXTRACT(HOUR FROM AVG(datetimes - time_difference)),
        'minutes', EXTRACT(MINUTE FROM AVG(datetimes - time_difference)),
        'seconds', EXTRACT(SECOND FROM AVG(datetimes - time_difference)),
        'milliseconds', EXTRACT(MILLISECOND FROM AVG(datetimes - time_difference))
    ) AS actual_time_taken 
FROM 
    cte2
GROUP BY 
    year
ORDER BY 
    (EXTRACT(EPOCH FROM AVG(datetimes - time_difference)) * 1000) DESC
LIMIT 5;
