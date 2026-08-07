-- ==========================================================
-- Display all customer records.
-- ==========================================================

SELECT *
FROM customers;

--- ==========================================================
-- Query 1
-- Total Revenue Per Category
--
-- Reason:
-- This query calculates the total revenue generated
-- from each product category after applying discounts.
-- Revenue Formula:
-- quantity × unit_price × (1 - discount_percent / 100)
-- ==========================================================

SELECT
    p.category,

    ROUND(
        SUM(
            oi.quantity *
            oi.unit_price *
            (1 - oi.discount_percent / 100.0)
        ),
        2
    ) AS total_revenue

FROM order_items oi

JOIN products p
ON oi.product_id = p.product_id

GROUP BY p.category

ORDER BY total_revenue DESC;



-- ==========================================================
-- Query 2
-- Top 10 Customers by Total Order Value
--
-- Reason:
-- This query calculates the total amount spent
-- by each customer after applying discounts.
-- The result is sorted in descending order
-- to identify the top 10 customers.
-- ==========================================================

SELECT

    c.customer_id,
    c.customer_name,

    ROUND(

        SUM(

            oi.quantity *
            oi.unit_price *
            (1 - oi.discount_percent / 100.0)

        ),

        2

    ) AS total_order_value

FROM customers c

INNER JOIN orders o
ON c.customer_id = o.customer_id

INNER JOIN order_items oi
ON o.order_id = oi.order_id

GROUP BY

    c.customer_id,
    c.customer_name

ORDER BY

    total_order_value DESC

LIMIT 10;



-- ==========================================================
-- Query 3
-- Month-wise Order Count (Last 12 Months)
--
-- Reason:
-- This query counts the total number of orders
-- placed in each month.
-- It helps analyze monthly order trends.
-- ==========================================================

SELECT

    strftime('%Y-%m', order_date) AS order_month,

    COUNT(order_id) AS total_orders

FROM orders

GROUP BY

    strftime('%Y-%m', order_date)

ORDER BY

    order_month DESC

LIMIT 12;


-- ==========================================================
-- Query 4
-- Customers Who Never Had Any Item Delivered
--
-- Reason:
-- This query finds customers who have placed
-- one or more orders but none of their orders
-- were marked as DELIVERED.
-- ==========================================================

SELECT

    c.customer_id,
    c.customer_name

FROM customers c

WHERE c.customer_id IN (

    SELECT customer_id
    FROM orders

)

AND c.customer_id NOT IN (

    SELECT customer_id
    FROM orders
    WHERE status = 'DELIVERED'

)

ORDER BY

    c.customer_name;




-- ==========================================================
-- Query 5
-- Products With More Returns Than Purchases
--
-- Reason:
-- This query compares the number of returned
-- orders with completed purchases for each product.
-- It helps identify products that have a
-- higher return rate than successful purchases.
-- ==========================================================

    SELECT

        p.product_id,
        p.product_name,

        SUM(

            CASE

                WHEN o.status = 'RETURNED'

                THEN 1

                ELSE 0

            END

        ) AS total_returns,

        SUM(

            CASE

                WHEN o.status = 'DELIVERED'

                THEN 1

                ELSE 0

            END

        ) AS total_purchases

    FROM products p

    INNER JOIN order_items oi
    ON p.product_id = oi.product_id

    INNER JOIN orders o
    ON oi.order_id = o.order_id

    GROUP BY

        p.product_id,
        p.product_name

    HAVING

        total_returns > total_purchases

    ORDER BY

        total_returns DESC;



-- ==========================================================
-- Query 6
-- Return Rate Per Category
--
-- Reason:
-- This query calculates the return rate
-- for each product category.
-- It helps identify categories having
-- higher product returns.
-- ==========================================================
SELECT
    p.category,

    SUM(
        CASE
            WHEN o.status = 'RETURNED' THEN 1
            ELSE 0
        END
    ) AS total_returns,

    COUNT(*) AS total_orders,

    ROUND(
        (
            SUM(
                CASE
                    WHEN o.status = 'RETURNED' THEN 1
                    ELSE 0
                END
            ) * 100.0
        ) / COUNT(*),
        2
    ) AS return_rate_percentage

FROM products p

INNER JOIN order_items oi
ON p.product_id = oi.product_id

INNER JOIN orders o
ON oi.order_id = o.order_id

GROUP BY p.category

ORDER BY return_rate_percentage DESC;



-- ==========================================================
-- Query 7
-- Running Total of Revenue
--
-- Reason:
-- This query calculates the running total
-- of revenue based on the order date.
-- It helps understand how revenue grows
-- over time.
-- ==========================================================

SELECT

    DATE(o.order_date) AS order_date,

    ROUND(

        SUM(

            oi.quantity *
            oi.unit_price *
            (1 - oi.discount_percent / 100.0)

        ),

        2

    ) AS daily_revenue,

    ROUND(

        SUM(

            SUM(

                oi.quantity *
                oi.unit_price *
                (1 - oi.discount_percent / 100.0)

            )

        ) OVER (

            ORDER BY DATE(o.order_date)

        ),

        2

    ) AS running_total

FROM orders o

INNER JOIN order_items oi
ON o.order_id = oi.order_id

GROUP BY

    DATE(o.order_date)

ORDER BY

    DATE(o.order_date);