from enum_models import TableNames, OrderStatus


STAGING_QUERIES = {
    TableNames.STAGING_RANKED_CUSTOMERS: f"""
    SELECT 
        foi.customer_unique_id,
        SUM(foi.price) AS total_spent,
        COUNT(DISTINCT foi.order_id) AS total_orders,
        MAX(foi.order_delivered_customer_date) AS last_order_date,
    FROM 
        {TableNames.FACTS_ORDER_ITEMS.value} foi
    WHERE foi.order_status = {OrderStatus.DELIVERED.value}
    GROUP BY foi.customer_unique_id
    """,
}