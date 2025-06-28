from enum_models import TableNames, OrderStatus

ANALYTIC_QUERIES = {
    TableNames.ANALYTICS_MOST_VALUABLE_CUSTOMERS: f"""
        SELECT 
            scd.customer_unique_id,
            SUM(scd.price) AS total_spent,
            COUNT(DISTINCT scd.order_id) AS total_orders,
            MAX(scd.order_delivered_customer_date) AS last_order_date,
            RANK() OVER (ORDER BY total_spent DESC) AS customer_rating
        FROM 
            {TableNames.STAGING_CUSTOMERS_DELIVERIES.value} scd
        WHERE scd.order_status = '{OrderStatus.DELIVERED.value}'
        GROUP BY 
        scd.customer_unique_id, 
        DATE_TRUNC('month', scd.order_delivered_customer_date)
        ORDER BY 
            customer_rating;
    """,
    TableNames.ANALYTICS_ROLLING_QUARTERS: f"""
        SELECT
            scd.customer_unique_id,
            DATE_TRUNC('month', scd.order_delivered_customer_date) AS month,
            SUM(price) AS monthly_total,
            AVG(monthly_total) OVER (
                PARTITION BY scd.customer_unique_id
                ORDER BY month
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS rolling_quartal_avg
        FROM {TableNames.STAGING_CUSTOMERS_DELIVERIES.value} scd
        GROUP BY 
            scd.customer_unique_id, 
            DATE_TRUNC('month', scd.order_delivered_customer_date)
        ORDER BY scd.customer_unique_id, month;
    """,
}
