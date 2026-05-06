-- Total spending
SELECT SUM(amount) AS total_spent FROM transactions;

-- Spending by category
SELECT category, SUM(amount) AS total_spent
FROM transactions
GROUP BY category
ORDER BY total_spent DESC;

-- Monthly spending
SELECT 
    strftime('%Y-%m', date) AS month,
    SUM(amount) AS total_spent
FROM transactions
GROUP BY month
ORDER BY month;

-- Budget vs Actual
SELECT 
    t.category,
    SUM(t.amount) AS total_spent,
    b.budget,
    (b.budget - SUM(t.amount)) AS remaining,
    CASE 
        WHEN SUM(t.amount) > b.budget THEN 'Over Budget'
        ELSE 'Within Budget'
    END AS status
FROM transactions t
JOIN budgets b
ON t.category = b.category
GROUP BY t.category;

