from enum_models import TableNames, OrderStatus


STAGING_QUERIES = {
    TableNames.STAGING_CUSTOMERS_DELIVERIES: f"""
    SELECT
        foi.customer_unique_id,
        foi.order_delivered_customer_date,
        foi.price,
        foi.order_id,
        foi.order_status,
        foi.product_id
    FROM
        {TableNames.FACTS_ORDER_ITEMS.value} foi
    """
}
