from dataclasses import dataclass

import duckdb

from enum_models import TableNames, TableOperations
from queries import (
    ANALYTIC_QUERIES,
    FACTS_QUERIES,
    SOURCE_QUERIES,
    STAGING_QUERIES,
)
from utils.create_sub_tables import (
    handle_geolocation,
    handle_order_payments,
    handle_order_reviews,
    handle_sellers,
)


@dataclass
class HandleOlist:
    connection: duckdb.DuckDBPyConnection

    def create_sub_tables(self) -> None:
        self.create_customer_table()
        self.create_order_items_table()
        self.create_order_table()
        self.create_products_table()
        self.create_product_category_name_translation_table()

        # # TODO change this to methods
        handle_geolocation(self.connection)
        handle_order_payments(self.connection)
        handle_order_reviews(self.connection)
        handle_sellers(self.connection)

    def create_facts_table(self) -> None:
        # create table
        query = FACTS_QUERIES[TableNames.FACTS_ORDER_ITEMS][TableOperations.CREATE]
        self.handle_query(query)

        #fill table
        insert_facts_table_query = FACTS_QUERIES[TableNames.FACTS_ORDER_ITEMS][TableOperations.INSERT]
        self.handle_query(insert_facts_table_query)

    def create_staging_tables(self) -> None:
        self.create_staging_customer_deliveries()

    def create_staging_customer_deliveries(self) -> None:
        table = TableNames.STAGING_CUSTOMERS_DELIVERIES
        select_query = STAGING_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def create_analytical_tables(self) -> None:
        self.create_most_valuable_customers()
        self.create_three_month_user_purchases()
        self.create_top_ten_products()
        self.create_raise_sales_gradient()
        self.create_cumulative_product_sales()
        self.create_daily_rebates()
        self.create_avg_comparison()

    def create_most_valuable_customers(self) -> None:
        table = TableNames.ANALYTICS_MOST_VALUABLE_CUSTOMERS
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def create_three_month_user_purchases(self) -> None:
        table = TableNames.ANALYTICS_ROLLING_QUARTERS
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def create_top_ten_products(self) -> None:
        # create top 10 products by sales in last quarter
        table = TableNames.ANALYTICS_TOP_TEN_PRODUCTS_Q_SALES
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

        # create top 10 products by sales in last quarter by category
        table = TableNames.ANALYTICS_TOP_TEN_PRODUCTS_Q_BY_CATEGORY
        select_query = ANALYTIC_QUERIES[table]
        self.create_table_from_select(select_query, table.value)

    def create_raise_sales_gradient(self) -> None:
        # create table with top 5 product categories by sales
        table = TableNames.ANALYTICS_RAISE_SALES_GRADIENT
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def create_cumulative_product_sales(self) -> None:
        # create cumulative product sales table
        table = TableNames.ANALYTICS_CUMULATIVE_PRODUCT_SALES
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)


    def create_daily_rebates(self) -> None:
        # create daily rebates table
        table = TableNames.ANALYTICS_DAILY_REBATES
        select_query = ANALYTIC_QUERIES[table]

        self.create_table_from_select(select_query, table.value)

    def create_table_from_select(self, select_query: str, table_name) -> None:
        result_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS
            {select_query}
        """
        self.handle_query(result_query)

    def insert_into_table(self, table_name: str, csv_path: str) -> None:
        insert_query = f"""
            INSERT INTO {table_name} 
                SELECT * FROM read_csv('{csv_path}')
            ON CONFLICT DO NOTHING
        """
        self.handle_query(insert_query)

    def handle_query(self, query: str) -> None:
        self.connection.sql(query)

    def create_customer_table(self) -> None:
        table = TableNames.SRC_CUSTOMERS
        sql_create = SOURCE_QUERIES[table][TableOperations.CREATE]
        self.handle_query(sql_create)

        self.handle_query(sql_create)
        self.insert_into_table(
            str(table.value),
            csv_path="dataset/olist_customers_dataset.csv",
        )

    def create_order_items_table(self) -> None:
        table_name = TableNames.SRC_ORDER_ITEMS.value
        sql_create = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            order_id VARCHAR(100),
            order_item_id INT,
            product_id VARCHAR(100),
            seller_id VARCHAR(100),
            shipping_limit_date TIMESTAMP,
            price FLOAT,
            freight_value FLOAT,
            UNIQUE (order_id, order_item_id)
        )
        """

        self.handle_query(sql_create)
        self.insert_into_table(
            str(table_name),
            csv_path="dataset/olist_order_items_dataset.csv",
        )

    def create_order_table(self) -> None:
        table_name = TableNames.SRC_ORDERS.value
        sql_create = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                order_id VARCHAR(100) PRIMARY KEY,
                customer_id VARCHAR(100),
                order_status VARCHAR(50),
                order_purchase_timestamp TIMESTAMP,
                order_approved_at TIMESTAMP,
                order_delivered_carrier_date TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP
            )
        """
        self.handle_query(sql_create)
        self.insert_into_table(
            str(table_name),
            csv_path="dataset/olist_orders_dataset.csv",
        )

    def create_products_table(self) -> None:
        table = TableNames.SRC_PRODUCTS
        sql_create = SOURCE_QUERIES[table][TableOperations.CREATE]

        self.handle_query(sql_create)
        self.insert_into_table(str(table.value), csv_path="dataset/olist_products_dataset.csv")

    def create_product_category_name_translation_table(self) -> None:
        table = TableNames.SRC_PRODUCTS_CATEGORIES_TRANSLATIONS
        sql_create = SOURCE_QUERIES[table][TableOperations.CREATE]

        self.handle_query(sql_create)
        self.insert_into_table(
            str(table.value),
            csv_path="dataset/product_category_name_translation.csv",
        )
