from dataclasses import dataclass
from enum import StrEnum

import duckdb

from queries import ANALYTIC_QUERIES
from utils.create_sub_tables import (
    handle_geolocation,
    handle_order_items,
    handle_order_payments,
    handle_order_reviews,
    handle_orders,
    handle_products,
    handle_sellers,
    handle_product_category_name_translation,
)


class TableNames(StrEnum):
    FACTS_ORDER_ITEMS = "facts_order_items"
    CUSTOMERS = "customers"


class OrderStatus(StrEnum):
    DELIVERED = "delivered"
    CANCELED = "canceled"
    PROCESSING = "processing"


@dataclass
class HandleOlist:
    connection: duckdb.DuckDBPyConnection

    def create_sub_tables(self) -> None:
        self.create_customer_table()
        handle_geolocation(self.connection)
        handle_order_items(self.connection)
        handle_order_payments(self.connection)
        handle_order_reviews(self.connection)
        handle_orders(self.connection)
        handle_products(self.connection)
        handle_sellers(self.connection)
        handle_product_category_name_translation(self.connection)

    def create_facts_table(self) -> None:
        query = self.create_queries[TableNames.FACTS_ORDER_ITEMS.value]
        self.handle_query(query)

    def fill_facts_table(self) -> None:
        insert_facts_table_query = f"""
            INSERT INTO {TableNames.FACTS_ORDER_ITEMS.value}
            SELECT
                ordi.order_id,
                ordi.order_item_id,
                ordi.product_id,
                ordi.seller_id,
                ord.customer_id,
                ordi.price,
                ord.order_status,
                ord.order_delivered_customer_date
            FROM read_csv('dataset/olist_order_items_dataset.csv') as ordi
            LEFT JOIN read_csv('dataset/olist_orders_dataset.csv') as ord
                ON ord.order_id = ordi.order_id
            """
        self.handle_query(insert_facts_table_query)

    def create_most_valuable_customers(self) -> None:
        """Base entity values are:
        - for total spent amount - 0,4
        - for total orders - 0,4
        - for last date - 0,2, 0,1, 0,5 or 0
        """
        select_query = ANALYTIC_QUERIES["user_rating"]
        result_query = f"""
            CREATE TABLE IF NOT EXISTS most_valuable_customers AS
            {select_query}
        """
        self.handle_query(result_query)

    def create_three_month_user_purchases(self) -> None:
        select_query = ANALYTIC_QUERIES["rolling_quarters"]
        result_query = f"""
            CREATE TABLE IF NOT EXISTS rolling_quarters AS
            {select_query}
        """
        self.handle_query(result_query)

    @property
    def create_queries(self) -> dict:
        create_facts_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TableNames.FACTS_ORDER_ITEMS.value} (
            order_id VARCHAR(36),
            order_item_id INT,
            product_id VARCHAR(36),
            seller_id VARCHAR(36),
            customer_id VARCHAR(36),
            price DECIMAL(10, 2),
            order_status VARCHAR(20),
            order_delivered_customer_date DATE
        )
        """
        return {
            TableNames.FACTS_ORDER_ITEMS: create_facts_table_query,
        }

    def handle_query(self, query: str) -> None:
        self.connection.sql(query)

    def create_customer_table(self) -> None:
        sql_create = f"""CREATE TABLE IF NOT EXISTS {TableNames.CUSTOMERS.value} (
            customer_id VARCHAR(100),
            customer_unique_id VARCHAR(100) UNIQUE,
            customer_zip_code_prefix VARCHAR(10),
            customer_city VARCHAR(100),
            customer_state VARCHAR(5)
        )
        """
        self.handle_query(sql_create)

        csv_path = "dataset/olist_customers_dataset.csv"
        insert_query = f"""
            INSERT INTO {TableNames.CUSTOMERS.value} 
                SELECT * FROM read_csv('{csv_path}')
            ON CONFLICT DO NOTHING
        """
        self.handle_query(insert_query)
