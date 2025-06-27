from enum_models import TableNames, OrderStatus

ANALYTIC_QUERIES = {
    TableNames.ANALYTICS_MOST_VALUABLE_CUSTOMERS: f"""
            WITH ranked_customers AS (
                SELECT 
                    foi.customer_unique_id,
                    SUM(foi.price) AS total_spent,
                    COUNT(DISTINCT foi.order_id) AS total_orders,
                    MAX(foi.order_delivered_customer_date) AS last_order_date,
                FROM 
                    facts_order_items foi
                WHERE foi.order_status = '{OrderStatus.DELIVERED.value}'
                GROUP BY foi.customer_unique_id
            )
            SELECT 
                rc.customer_unique_id,
                rc.total_spent,
                rc.total_orders,
                rc.last_order_date,
                RANK() OVER (ORDER BY total_spent DESC) AS customer_rating
            FROM 
                ranked_customers rc
            ORDER BY 
                customer_rating;
        """,
    TableNames.ANALYTICS_ROLLING_QUARTERS: """
        WITH monthly_totals AS (
            SELECT
                foi.customer_unique_id,
                DATE_TRUNC('month', foi.order_delivered_customer_date) AS month,
                SUM(price) AS monthly_total
            FROM facts_order_items foi
            GROUP BY customer_unique_id, DATE_TRUNC('month', foi.order_delivered_customer_date)
        )
        SELECT
            customer_unique_id,
            month,
            monthly_total,
            AVG(monthly_total) OVER (
                PARTITION BY customer_unique_id
                ORDER BY month
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS rolling_quartal_avg
        FROM monthly_totals
        ORDER BY customer_unique_id, month;
    """,
}
