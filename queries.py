ANALYTIC_QUERIES = {
    "most_valuable_customers": """
            WITH ranked_customers AS (
                SELECT 
                    foi.customer_id,
                    SUM(foi.price) AS total_spent,
                    COUNT(DISTINCT foi.order_id) AS total_orders,
                    MAX(foi.order_delivered_customer_date) AS last_order_date,
                    (total_spent * 0.4) + (total_orders * 0.4) + 
                    (
                        CASE 
                            WHEN last_order_date >= CURRENT_DATE - INTERVAL '30 days' THEN 20
                            WHEN last_order_date >= CURRENT_DATE - INTERVAL '90 days' THEN 10
                            WHEN last_order_date >= CURRENT_DATE - INTERVAL '180 days' THEN 5
                            ELSE 0
                        END
                    ) AS composite_score
                FROM 
                    facts_order_items foi
                WHERE foi.order_status = 'delivered'
                GROUP BY foi.customer_id
            )
            SELECT 
                rc.customer_id,
                rc.total_spent,
                rc.total_orders,
                rc.last_order_date,
                RANK() OVER (ORDER BY composite_score DESC) AS customer_rating
            FROM 
                ranked_customers rc
            ORDER BY 
                customer_rating;
        """,
    "rolling_quarters": """
        WITH monthly_totals AS (
            SELECT
                foi.customer_id AS customer_id,
                DATE_TRUNC('month', foi.order_delivered_customer_date) AS month,
                SUM(price) AS monthly_total
            FROM facts_order_items foi
            GROUP BY customer_id, DATE_TRUNC('month', foi.order_delivered_customer_date)
        )
        SELECT
            customer_id,
            month,
            monthly_total,
            AVG(monthly_total) OVER (
                PARTITION BY customer_id
                ORDER BY month
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS rolling_quartal_avg
        FROM monthly_totals
        ORDER BY customer_id, month;
    """,
}
