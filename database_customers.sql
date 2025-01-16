CREATE DATABASE IF NOT EXISTS data_analytics_project;
USE data_analytics_project;

-- Table: customers
CREATE TABLE customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(30),
    date_joined datetime
);

-- Table: products
CREATE TABLE products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10, 2) NOT NULL,
    stock INT NOT NULL DEFAULT 0
);

-- Table: personal_information
CREATE TABLE personal_information (
    personal_info_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    address VARCHAR(255),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(30),
    country VARCHAR(50),
    is_working bool,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
);

-- Additional Table: orders
CREATE TABLE orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    order_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    quantity INT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
);
set innodb_lock_wait_timeout=300;

ALTER TABLE personal_information
DROP COLUMN country,
DROP COLUMN postal_code;


select  * from customers;
select  * from products;
select  * from personal_information;
select  * from orders;


select sum(price) as total_price, category from products
group by category;

select concat(c.first_name, c.last_name) as full_name, o.quantity from customers c 
left join orders o on o.customer_id = c.customer_id
order by quantity desc
limit 10;


--  Calculate revenue by product category
SELECT 
    p.category,
    SUM(p.price * o.quantity) AS total_revenue
FROM 
    products p
JOIN 
    orders o ON p.product_id = o.product_id
GROUP BY 
    p.category
ORDER BY 
    total_revenue DESC;

-- Find top customers contributing to revenue
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    p.category,
    SUM(p.price * o.quantity) AS customer_revenue
FROM 
    customers c
JOIN 
    orders o ON c.customer_id = o.customer_id
JOIN 
    products p ON o.product_id = p.product_id
GROUP BY 
    c.customer_id, p.category
ORDER BY 
    customer_revenue DESC
LIMIT 10;

-- Calulcate the distribution of workers influence on count of orders
SELECT pi.is_working, COUNT(o.order_id) AS total_orders
FROM personal_information pi
JOIN customers c ON pi.customer_id = c.customer_id
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY pi.is_working;

-- Find most popular orders 
SELECT p.category, COUNT(o.order_id) AS order_count
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY order_count DESC;



-- Find Customers Who Haven’t Made Any Purchases in the Last 6 Months:
SELECT c.first_name, c.last_name
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.order_date >= CURDATE() - INTERVAL 6 MONTH
WHERE o.customer_id IS NULL;

-- Find the Most Expensive Product Ordered by Each Customer
SELECT c.first_name, c.last_name, p.product_name, o.quantity, p.price
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN products p ON o.product_id = p.product_id
WHERE o.order_id IN (
    SELECT o2.order_id
    FROM orders o2
    JOIN products p2 ON o2.product_id = p2.product_id
    WHERE o2.customer_id = c.customer_id
    ORDER BY p2.price DESC
);


-- Rank Customers by Total Quantity Ordered

SELECT first_name, last_name, SUM(o.quantity) AS total_quantity,
       ROW_NUMBER() OVER (ORDER BY SUM(o.quantity) DESC) AS ranking
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id
ORDER BY ranking;

-- Calculate Running Total of Orders by Customer
SELECT c.first_name, c.last_name, o.order_date, o.quantity,
       SUM(o.quantity) OVER (PARTITION BY c.customer_id ORDER BY o.order_date) AS running_total
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
ORDER BY c.customer_id, o.order_date;

-- Get the Average Price of Products Ordered by Each Customer
SELECT c.first_name, c.last_name, AVG(p.price) OVER (PARTITION BY c.customer_id) AS avg_price_ordered
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN products p ON o.product_id = p.product_id
ORDER BY c.customer_id;



-- Find the Top 5 Customers by Total Order Quantity 
SELECT first_name, last_name, total_quantity,
       ROW_NUMBER() OVER (ORDER BY total_quantity DESC) AS ranking
FROM (
    SELECT c.first_name, c.last_name, SUM(o.quantity) AS total_quantity
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
) AS customer_totals
WHERE ranking <= 5;


-- Find the Customer with the Most Expensive Order
SELECT first_name, last_name, order_date, total_price
FROM (
    SELECT c.first_name, c.last_name, o.order_date,
           SUM(o.quantity * p.price) AS total_price,
           RANK() OVER (ORDER BY SUM(o.quantity * p.price) DESC) AS price_rank
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN products p ON o.product_id = p.product_id
    GROUP BY o.order_id
) AS ranked_orders
WHERE price_rank = 1;



WITH MonthlySales AS (
    SELECT 
        CONCAT(customers.first_name, ' ', customers.last_name) AS full_name,
        SUM(orders.quantity * products.price) AS total_price,
        MONTH(orders.order_date) AS sale_month
    FROM 
        orders
    INNER JOIN 
        customers ON customers.customer_id = orders.customer_id
    INNER JOIN 
        products ON products.product_id = orders.product_id
    GROUP BY 
        customers.first_name, customers.last_name, MONTH(orders.order_date)
),
RankedSales AS (
    SELECT 
        full_name, 
        total_price, 
        sale_month,
        RANK() OVER (PARTITION BY sale_month ORDER BY total_price DESC) AS rank_in_month
    FROM 
        MonthlySales
)
SELECT 
    full_name, 
    total_price, 
    sale_month
FROM 
    RankedSales
WHERE 
    rank_in_month = 1;


# Query with Difference in total costs per Months and total costs:
select 
SUM(o.quantity * p.price) as total_cost, 
count(o.order_id) as total_orders,
SUM(o.quantity * p.price) - lag(SUM(o.quantity * p.price)) over (ORDER BY MONTH(o.order_date)) AS month_difference,
month(o.order_date) as months from orders o 
inner join products p on p.product_id = o.product_id
group by month(o.order_date)
order by month(o.order_date) desc;


# customer spendings analysis and bonus system with cashback
SELECT c.customer_id, 
       SUM(o.quantity * p.price) AS total_spending_per_person,
       CASE 
           WHEN SUM(o.quantity * p.price) >= 8000 THEN SUM(o.quantity * p.price) - SUM(o.quantity * p.price) * 0.2
           WHEN SUM(o.quantity * p.price) BETWEEN 5000 AND 7999 THEN SUM(o.quantity * p.price) - SUM(o.quantity * p.price) * 0.1
           WHEN SUM(o.quantity * p.price) BETWEEN 1000 AND 4999 THEN SUM(o.quantity * p.price) - SUM(o.quantity * p.price) * 0.05
           ELSE SUM(o.quantity * p.price)
       END AS minus_bonus_cashback
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
INNER JOIN products p ON p.product_id = o.product_id
WHERE o.order_date >= '2024-01-01' AND o.order_date < '2025-01-01'
GROUP BY c.customer_id;

