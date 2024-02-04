
-- Task 1
SELECT country_code AS country,
	COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country
ORDER BY total_no_stores DESC;



-- Task 2
SELECT locality AS locality,
	COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;


-- SELECT * FROM dim_store_details