-- Q1. Display all customers

SELECT *
FROM customers;

-- Q2. Display first name, last name and city

SELECT first_name,
       last_name,
       city
FROM customers;

-- Q3. Unique product categories

SELECT DISTINCT category
FROM products;

-- Q4. Identify the Primary Key of each table.
SHOW KEYS FROM customers
WHERE Key_name = 'PRIMARY';

SHOW KEYS FROM products
WHERE Key_name = 'PRIMARY';

SHOW KEYS FROM orders
WHERE Key_name = 'PRIMARY';

SHOW KEYS FROM order_items
WHERE Key_name = 'PRIMARY';

/*
Customers   -> customer_id
Products    -> product_id
Orders      -> order_id
Order_Items -> item_id

A Primary Key uniquely identifies every record in a table.
It cannot contain duplicate values or NULL values.
*/

--Q5. What constraints are applied to the email column?
/*
The email column has:

1. NOT NULL
2. UNIQUE

NOT NULL prevents empty values.

UNIQUE prevents duplicate email addresses.
*/

--Q6. Try inserting a product with unit_price = -50.

INSERT INTO products
VALUES
(209,'Test Product','Electronics','TestBrand',-50,100);
--error: CHECK constraint failed
/*
The CHECK constraint

CHECK(unit_price > 0)

does not allow negative prices.

Therefore MySQL rejects the INSERT statement.
*/
