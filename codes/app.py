import streamlit as st
import sqlite3
import pandas as pd
conn = sqlite3.connect("C:/Users/Lenovo/Personal Finance Dashboard/codes/finance.db")

st.title("💰 Personal Finance Dashboard")


total_query = "SELECT SUM(amount) as total FROM transactions"
total = pd.read_sql_query(total_query, conn)

st.metric("Total Spending", f"{total['total'][0]:,.2f}")

st.subheader("Spending by Category")

cat_query = """
SELECT category, SUM(amount) as total
FROM transactions
GROUP BY category
ORDER BY total DESC
"""

cat_df = pd.read_sql_query(cat_query, conn)
st.bar_chart(cat_df.set_index('category'))

#Monthly Spending

st.subheader("Monthly Spending")

month_query = """
SELECT strftime('%Y-%m', date) as month, SUM(amount) as total
FROM transactions
GROUP BY month
ORDER BY month
"""

month_df = pd.read_sql_query(month_query, conn)
st.line_chart(month_df.set_index('month'))


#Budgeting

st.subheader("Budget Status")

budget_query = """
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
GROUP BY t.category
"""

budget_df = pd.read_sql_query(budget_query, conn)

st.dataframe(budget_df)

# Highlight over budget
st.subheader("⚠️ Over Budget Categories")
over_budget = budget_df[budget_df['status'] == 'Over Budget']

if not over_budget.empty:
    st.write(over_budget)
else:
    st.success("You're within budget 🎉")