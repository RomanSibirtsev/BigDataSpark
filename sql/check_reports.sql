SELECT 'mart_products' AS mart, count() AS rows FROM analytics.mart_products
UNION ALL SELECT 'mart_customers', count() FROM analytics.mart_customers
UNION ALL SELECT 'mart_time', count() FROM analytics.mart_time
UNION ALL SELECT 'mart_stores', count() FROM analytics.mart_stores
UNION ALL SELECT 'mart_suppliers', count() FROM analytics.mart_suppliers
UNION ALL SELECT 'mart_quality', count() FROM analytics.mart_quality;

SELECT * FROM analytics.mart_products ORDER BY sales_rank LIMIT 10;
SELECT DISTINCT product_category, category_revenue
FROM analytics.mart_products
ORDER BY product_category;
SELECT * FROM analytics.mart_customers ORDER BY spend_rank LIMIT 10;
SELECT country, max(country_customer_count) AS customer_count
FROM analytics.mart_customers
GROUP BY country
ORDER BY country;
SELECT * FROM analytics.mart_time ORDER BY period_type, period_start;
SELECT * FROM analytics.mart_stores ORDER BY revenue_rank LIMIT 5;
SELECT city, country, max(city_revenue) AS city_revenue
FROM analytics.mart_stores
GROUP BY city, country
ORDER BY country, city;
SELECT country, max(country_revenue) AS country_revenue
FROM analytics.mart_stores
GROUP BY country
ORDER BY country;
SELECT * FROM analytics.mart_suppliers ORDER BY revenue_rank LIMIT 5;
SELECT supplier_country, max(country_revenue) AS country_revenue
FROM analytics.mart_suppliers
GROUP BY supplier_country
ORDER BY supplier_country;
SELECT * FROM analytics.mart_quality ORDER BY best_rating_rank LIMIT 10;
SELECT * FROM analytics.mart_quality ORDER BY worst_rating_rank LIMIT 10;
SELECT * FROM analytics.mart_quality ORDER BY review_count_rank LIMIT 10;
