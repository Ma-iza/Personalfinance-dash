--Total spending
SELECT SUM(amount) AS total_spent
FROM transactions;

--Total transactions
SELECT COUNT(*) AS total_transactions
FROM transactions;

--Spending based on categories
SELECT 
    category, 
    SUM(amount) AS total_spent
FROM transactions
GROUP BY category
ORDER BY total_spent DESC;

--monthly spending
SELECT 
    strftime('%Y-%m', date) AS month,
    SUM(amount) AS total_spent
FROM transactions
GROUP BY month
ORDER BY month;

