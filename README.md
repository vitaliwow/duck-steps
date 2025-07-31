### Original dataset
The original dataset is available at [https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
Save dataset files to `dataset/`

Requirements:

In DuckDB, it will need to be transformed according to the medallion architecture. For now, the main task is simply to define the tables. For the gold layer, the following tables need to be created:  

1. **Identifying our most valuable customers**: Can you create a report showing each customer's total spending, total number of orders, and the date of their last order? We would also like to see a ranking for each customer based on their total spending.  

2. **Understanding customer behavior over time for marketing campaigns**: Can you calculate a 3-month rolling average of sales for each customer?  

3. **Determining the average time between orders for each customer** to understand purchasing cycles.  

4. **Identifying our top 10 best-selling products overall and in each product category by revenue for the last quarter**.  

5. **Calculating the month-over-month sales growth percentage for our top 5 product categories**.  

6. **For inventory planning, show the cumulative sales of each product for the current year**.  

7. **What is the daily total sales revenue for the last month?**  

8. **How does the average order value compare to the previous month's average order value for each state?**  

9. **Evaluating our salespeople based on their total sales revenue and average rating over the past year**. We want to identify top performers and those who may need support.  

10. **Showing the trend in the number of unique products sold by each salesperson by month**.
