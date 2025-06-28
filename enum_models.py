from enum import StrEnum


class TableNames(StrEnum):
    # Source tables
    SRC_CUSTOMERS = "src_customers"
    SRC_ORDER_ITEMS = "src_order_items"
    SRC_ORDERS = "src_orders"
    # Facts tables
    FACTS_ORDER_ITEMS = "facts_order_items"
    # Staging tables
    STAGING_CUSTOMERS_DELIVERIES = "staging_customers_deliveries"
    # Analytics tables
    ANALYTICS_MOST_VALUABLE_CUSTOMERS = "analytics_most_valuable_customers"
    ANALYTICS_ROLLING_QUARTERS = "analytics_rolling_quarters"
    ANALYTICS_TOP_PRODUCTS = "analytics_top_products"


class TableOperations(StrEnum):
    CREATE = "create"
    INSERT = "insert"


class OrderStatus(StrEnum):
    DELIVERED = "delivered"
    CANCELED = "canceled"
    PROCESSING = "processing"
