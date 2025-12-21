-- 1. Progress Over Time
SELECT 
    DATETRUNC(MONTH, order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
ORDER BY order_month


-- 2. Cumulative Monthly Analysis (Running Total & Moving Average)
SELECT 
    order_date, 
    order_year,
    total_sales,
    SUM(total_sales) OVER (PARTITION BY YEAR(order_date) ORDER BY order_date) AS monthly_running_total_sales,
    AVG(avg_sales) OVER (ORDER BY order_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3_months
FROM
(
    SELECT  
        DATETRUNC(MONTH, order_date) AS order_date, 
        YEAR(order_date) AS order_year,
        SUM(sales_amount) AS total_sales,
        AVG(sales_amount) AS avg_sales
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY 
        DATETRUNC(MONTH, order_date),
        YEAR(order_date)
) AS monthly_running_sales
ORDER BY order_date

-- 3. Performance Analytics (Year on Year & Month on Month)
-- Year on Year

WITH yearly_product_sales AS (
    SELECT 
        YEAR(f.order_date) AS order_year, 
        p.product_name, 
        p.category, 
        p.subcategory,
        SUM(f.sales_amount) AS current_year_sales
    FROM gold.fact_sales f LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
    WHERE order_date IS NOT NULL
    GROUP BY 
        YEAR(f.order_date),
        p.product_name,
        p.category,
        p.subcategory
)
SELECT 
    order_year,
    product_name,
    category,
    subcategory,
    current_year_sales, 
    LAG(current_year_sales) OVER (PARTITION BY product_name ORDER BY order_year) previous_year_sales,
    current_year_sales - LAG(current_year_sales) OVER (PARTITION BY product_name ORDER BY order_year) diff_previous_year,
    CASE WHEN current_year_sales - LAG(current_year_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
         WHEN current_year_sales - LAG(current_year_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
         ELSE 'No Change'
    END AS pevious_year_change,
    AVG(current_year_sales) OVER (PARTITION BY product_name) avg_sales,
    current_year_sales - AVG(current_year_sales) OVER (PARTITION BY product_name) diff_avg_sales,
    CASE WHEN current_year_sales - AVG(current_year_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
         WHEN current_year_sales - AVG(current_year_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
         ELSE 'Avg'
    END AS avg_change
FROM yearly_product_sales
ORDER BY product_name, order_year

-- Month on Month
WITH monthly_product_sales AS (
    SELECT 
        DATETRUNC(MONTH, f.order_date) AS order_month, 
        p.product_name, 
        SUM(f.sales_amount) AS current_month_sales
    FROM gold.fact_sales f LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
    WHERE order_date IS NOT NULL
    GROUP BY 
        MONTH(f.order_date),
        DATETRUNC(MONTH, f.order_date),
        p.product_name
)
SELECT 
    order_month, 
    product_name, 
    current_month_sales, 
    LAG(current_month_sales) OVER (PARTITION BY product_name ORDER BY order_month) previous_month_sales,
    current_month_sales - LAG(current_month_sales) OVER (PARTITION BY product_name ORDER BY order_month) diff_previous_month,
    CASE WHEN current_month_sales - LAG(current_month_sales) OVER (PARTITION BY product_name ORDER BY order_month) > 0 THEN 'Increase'
         WHEN current_month_sales - LAG(current_month_sales) OVER (PARTITION BY product_name ORDER BY order_month) < 0 THEN 'Decrease'
         ELSE 'No Change'
    END AS pevious_month_change,
    AVG(current_month_sales) OVER (PARTITION BY product_name) avg_sales,
    current_month_sales - AVG(current_month_sales) OVER (PARTITION BY product_name) diff_avg_sales,
    CASE WHEN current_month_sales - AVG(current_month_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
         WHEN current_month_sales - AVG(current_month_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
         ELSE 'Avg'
    END AS avg_change
FROM monthly_product_sales
ORDER BY product_name, order_month

-- 4. Part-to-Whole Analysis

WITH category_cont AS (
    SELECT 
        category, 
        subcategory,
        SUM(sales_amount) total_sales,
        COUNT(DISTINCT f.product_key) total_orders,
        COUNT(DISTINCT f.customer_key) total_customers
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
    LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key 
    GROUP BY 
        category, 
        subcategory
)
SELECT 
    category, 
    subcategory,
    total_sales,
    SUM(total_sales) OVER () overall_sales,
    CONCAT(ROUND((CAST(total_sales AS FLOAT) / SUM(total_sales) OVER ()) * 100, 2), '%') AS pct_of_total_sales,
    total_orders,
    SUM(total_orders) OVER () overall_orders,
    CONCAT(ROUND((CAST(total_orders AS FLOAT) / SUM(total_orders) OVER ()) * 100, 2), '%') AS pct_of_total_orders,
    total_customers,
    SUM(total_customers) OVER () overall_customers,
    CONCAT(ROUND((CAST(total_customers AS FLOAT) / SUM(total_customers) OVER ()) * 100, 2), '%') AS pct_of_total_customers
FROM category_cont
ORDER BY total_sales, total_orders, total_customers

-- 5. Data Segmentation (Customers)

WITH customer_spending AS (
    SELECT 
        c.customer_key,
        SUM(f.sales_amount) total_spending,
        MIN(order_date) first_order,
        MAX(order_date) last_order, 
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
    FROM gold.fact_sales f LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key 
    GROUP BY c.customer_key
)
SELECT 
    customer_segment, 
    COUNT(customer_key)  AS total_customers
FROM(
    SELECT 
        customer_key,
        CASE WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
            WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment
        FROM customer_spending
) AS t
GROUP BY customer_segment

-- 6. Reporting
-- a. Customer Report

DROP VIEW if EXISTS gold.report_customers

CREATE VIEW gold.report_customers AS

WITH base_query AS(
    SELECT 
        f.order_number, 
        f.product_key, 
        f.order_date, 
        f.sales_amount, 
        f.quantity,
        c.customer_key, 
        c.customer_number,
        CONCAT(first_name, ' ', c.last_name) customer_name,
        DATEDIFF(YEAR, c.birthdate, GETDATE()) age
    FROM gold.fact_sales f LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
    WHERE order_date IS NOT NULL
)
,customer_aggregation AS(
    SELECT 
        customer_key, 
        customer_number, 
        customer_name, 
        age,
        COUNT (DISTINCT order_number) total_orders,
        SUM (sales_amount) total_sales,
        SUM (quantity) total_quantity,
        COUNT (DISTINCT product_key) total_products,
        MAX(order_date) last_order_date,
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) lifespan
    FROM base_query AS b
    GROUP BY customer_key, 
        customer_number, 
        customer_name, 
        age
)
SELECT 
    customer_key, 
    customer_number, 
    customer_name, 
    age,
    CASE WHEN age < 20 THEN 'Under 20'
        WHEN age BETWEEN 20 AND 29 THEN '20-29'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 49 THEN '40-49'
        WHEN age BETWEEN 50 AND 59 THEN '50-59'
        ELSE '60 and above'
    END AS age_group,
    total_orders, 
    total_quantity,
    total_products,
    total_sales,
    CASE WHEN total_sales = 0 THEN 0
        ELSE total_sales / total_orders END avg_order_value,
    CASE WHEN lifespan = 0 THEN total_sales
        ELSE total_sales / lifespan END avg_monthly_spent,
    CASE WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,
    DATEDIFF(MONTH, last_order_date, GETDATE()) recency
FROM customer_aggregation


SELECT * FROM gold.report_customers

SELECT age_group, COUNT(customer_number) total_customers, SUM(total_sales) total_sales 
FROM gold.report_customers
GROUP BY age_group

SELECT customer_segment, COUNT(customer_number) total_customers, SUM(total_sales) total_sales 
FROM gold.report_customers
GROUP BY customer_segment


-- b. Product Report
IF OBJECT_ID('gold.report_products', 'V') IS NOT NULL
    DROP VIEW gold.report_products;
GO

CREATE VIEW gold.report_products AS
WITH base_query AS (
    SELECT
	    f.order_number,
        f.order_date,
		f.customer_key,
        f.sales_amount,
        f.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
    WHERE order_date IS NOT NULL
),

product_aggregations AS (
SELECT
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
    MAX(order_date) AS last_sale_date,
    COUNT(DISTINCT order_number) AS total_orders,
	COUNT(DISTINCT customer_key) AS total_customers,
    SUM(sales_amount) AS total_sales,
    SUM(quantity) AS total_quantity,
	ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)),1) AS avg_selling_price
FROM base_query

GROUP BY
    product_key,
    product_name,
    category,
    subcategory,
    cost
)

SELECT 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_sale_date,
	DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_in_months,
	CASE
		WHEN total_sales > 50000 THEN 'High-Performer'
		WHEN total_sales >= 10000 THEN 'Mid-Range'
		ELSE 'Low-Performer'
	END AS product_segment,
	lifespan,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	CASE 
		WHEN total_orders = 0 THEN 0
		ELSE total_sales / total_orders
	END AS avg_order_revenue,
	CASE
		WHEN lifespan = 0 THEN total_sales
		ELSE total_sales / lifespan
	END AS avg_monthly_revenue

FROM product_aggregations 

SELECT * FROM gold.report_products