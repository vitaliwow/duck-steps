from dataclasses import dataclass
from enum import StrEnum

import duckdb

from utils.create_sub_tables import handle_geolocation, handle_order_items, handle_order_payments, \
    handle_order_reviews, handle_orders, handle_products, handle_sellers, handle_product_category_name_translation


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

    def create_most_valuable_customers_dim(self) -> None:
        rfm_query = """
        WITH analysis_date AS (
            SELECT DATE '2025-03-19' AS today
        ),
        rfm_base AS (
            SELECT
                ord.customer_id,
                MAX(ord.order_delivered_customer_date) AS last_order_date,
                COUNT(*) AS frequency,
                SUM(oi.price) AS monetary
            FROM orders AS ord
            INNER JOIN order_items AS oi 
                ON oi.order_id = ord.order_id
            GROUP BY ord.customer_id
            HAVING last_order_date IS NOT NULL
        ),
        rfm_metrics AS (
            SELECT
                b.customer_id,
                DATE_PART('day', a.today - b.last_order_date) AS recency,
                b.frequency,
                b.monetary
            FROM rfm_base b
            CROSS JOIN analysis_date a
        ),
        percentiles AS (
            SELECT
                PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY recency) AS recency_p20,
                PERCENTILE_CONT(0.4) WITHIN GROUP (ORDER BY recency) AS recency_p40,
                PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY recency) AS recency_p60,
                PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY recency) AS recency_p80,
                PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY frequency) AS frequency_p20,
                PERCENTILE_CONT(0.4) WITHIN GROUP (ORDER BY frequency) AS frequency_p40,
                PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY frequency) AS frequency_p60,
                PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY frequency) AS frequency_p80,
                PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY monetary) AS monetary_p20,
                PERCENTILE_CONT(0.4) WITHIN GROUP (ORDER BY monetary) AS monetary_p40,
                PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY monetary) AS monetary_p60,
                PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY monetary) AS monetary_p80
            FROM rfm_metrics
        ),
        rfm_binned AS (
            SELECT
                r.customer_id,
                CASE
                    WHEN r.recency <= p.recency_p20 THEN 5
                    WHEN r.recency <= p.recency_p40 THEN 4
                    WHEN r.recency <= p.recency_p60 THEN 3
                    WHEN r.recency <= p.recency_p80 THEN 2
                    ELSE 1
                END AS r_score,
                CASE
                    WHEN r.frequency <= p.frequency_p20 THEN 1
                    WHEN r.frequency <= p.frequency_p40 THEN 2
                    WHEN r.frequency <= p.frequency_p60 THEN 3
                    WHEN r.frequency <= p.frequency_p80 THEN 4
                    ELSE 5
                END AS f_score,
                CASE
                    WHEN r.monetary <= p.monetary_p20 THEN 1
                    WHEN r.monetary <= p.monetary_p40 THEN 2
                    WHEN r.monetary <= p.monetary_p60 THEN 3
                    WHEN r.monetary <= p.monetary_p80 THEN 4
                    ELSE 5
                END AS m_score
            FROM rfm_metrics r
            CROSS JOIN percentiles p
        )
        SELECT
            customer_id,
            ROUND((r_score + f_score + m_score) / 15 * 10,2) as rating
        FROM rfm_binned;
        """

        rfm_results = self.connection.sql(query=rfm_query)
        self.connection.register("rfm_binned", rfm_results)

        result_query = """
            CREATE TABLE IF NOT EXISTS most_valuable_customers AS
            SELECT
                foi.customer_id,
                SUM(foi.price) AS total_spent,
                COUNT(DISTINCT foi.order_id) AS total_orders, 
                MAX(foi.order_delivered_customer_date) AS last_order_date,
                rfm.rating
            FROM facts_order_items foi
            INNER JOIN rfm_binned rfm
                ON rfm.customer_id = foi.customer_id
            WHERE 
                foi.order_status = 'delivered'
                AND foi.order_delivered_customer_date IS NOT NULL
            GROUP BY foi.customer_id, rfm.rating
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
