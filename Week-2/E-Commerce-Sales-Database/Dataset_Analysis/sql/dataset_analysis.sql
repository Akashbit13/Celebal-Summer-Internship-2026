/*
==========================================
Project : E-Commerce Sales Dataset Analysis
Dataset : SampleSuperstore.csv
Database : MySQL
==========================================
*/

-- ==========================================
-- Step 1 : Create Database
-- ==========================================

CREATE DATABASE superstore_db;

USE superstore_db;

-- ==========================================
-- Step 2 : Create Table
-- ==========================================

-- Table created using MySQL Table Data Import Wizard.


-- ==========================================
-- Step 3 : Load Dataset
-- ==========================================

-- Dataset imported using MySQL Table Data Import Wizard.


-- ==========================================
-- Step 4 : Explore Dataset
-- ==========================================

SHOW TABLES;

DESC superstore;

SELECT *
FROM superstore
LIMIT 10;

SELECT COUNT(*) AS Total_Records
FROM superstore;


-- ==========================================
-- Step 5 : Filtering
-- ==========================================

-- Region Filter
SELECT *
FROM superstore
WHERE region = 'West';

-- Category Filter
SELECT *
FROM superstore
WHERE category = 'Furniture';

-- Sales Greater Than 1000
SELECT *
FROM superstore
WHERE sales > 1000;

-- Profit Less Than 0
SELECT *
FROM superstore
WHERE profit < 0;

-- Date Filter
SELECT *
FROM superstore
WHERE order_date BETWEEN '2016-01-01' AND '2016-12-31';



-- ==========================================
-- Step 6 : Aggregation
-- ==========================================

-- Category-wise Sales
SELECT category,
SUM(sales) AS Total_Sales
FROM superstore
GROUP BY category;

-- Region-wise Average Sales
SELECT region,
AVG(sales) AS Average_Sales
FROM superstore
GROUP BY region;

-- Segment-wise Order Count
SELECT segment,
COUNT(*) AS Total_Orders
FROM superstore
GROUP BY segment;

-- Category-wise Quantity Sold
SELECT category,
SUM(quantity) AS Total_Quantity
FROM superstore
GROUP BY category;


-- ==========================================
-- Step 7 : Business Queries
-- ==========================================

-- Top 10 Products by Sales
SELECT product_name,
SUM(sales) AS Total_Sales
FROM superstore
GROUP BY product_name
ORDER BY Total_Sales DESC
LIMIT 10;

-- Top 10 Customers by Sales
SELECT customer_name,
SUM(sales) AS Total_Sales
FROM superstore
GROUP BY customer_name
ORDER BY Total_Sales DESC
LIMIT 10;

-- Top 5 Cities by Profit
SELECT city,
SUM(profit) AS Total_Profit
FROM superstore
GROUP BY city
ORDER BY Total_Profit DESC
LIMIT 5;

-- Monthly Sales Trend
SELECT MONTH(order_date) AS Month,
       SUM(sales) AS Total_Sales
FROM superstore
GROUP BY MONTH(order_date)
ORDER BY Month;

-- Duplicate Orders
SELECT order_id,
COUNT(*) AS Duplicate_Count
FROM superstore
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Highest Profit Products
SELECT product_name,
SUM(profit) AS Total_Profit
FROM superstore
GROUP BY product_name
ORDER BY Total_Profit DESC
LIMIT 10;


-- ==========================================
-- Step 8 : Validation
-- ==========================================

-- Total Rows
SELECT COUNT(*) AS Total_Rows
FROM superstore;

-- Check NULL Sales
SELECT COUNT(*) AS Null_Sales
FROM superstore
WHERE sales IS NULL;

-- Check NULL Profit
SELECT COUNT(*) AS Null_Profit
FROM superstore
WHERE profit IS NULL;

-- Check Duplicate Order IDs
SELECT order_id,
COUNT(*) AS Duplicate_Count
FROM superstore
GROUP BY order_id
HAVING COUNT(*) > 1;


-- ==========================================
-- Step 9 : Insights
-- ==========================================

-- Insights:
-- 1. Dataset contains 9994 records.
-- 2. West region contributes significantly to total sales.
-- 3. Technology products generate high sales.
-- 4. Some orders have negative profit.
-- 5. Duplicate Order IDs indicate multiple products in a single order.
