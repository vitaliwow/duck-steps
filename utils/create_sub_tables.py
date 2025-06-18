import duckdb


def handle_table(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    sql_create: str,
    csv_path: str,
):
    connection.sql(sql_create)
    connection.sql(f"INSERT INTO {table_name} SELECT * FROM read_csv('{csv_path}') ON CONFLICT DO NOTHING"
    )
    connection.table(table_name).show()


def handle_geolocation(connection: duckdb.DuckDBPyConnection):
    table_name = "geolocation"
    csv_path = "dataset/olist_geolocation_dataset.csv"

    sql_create = f"""CREATE TABLE IF NOT EXISTS {table_name} (
        geolocation_zip_code_prefix VARCHAR,
        geolocation_lat FLOAT,
        geolocation_lng FLOAT,
        geolocation_city VARCHAR,
        geolocation_state VARCHAR(5),
        UNIQUE (geolocation_lat, geolocation_lng)
    )
    """
    return handle_table(connection, table_name, sql_create, csv_path)


def handle_order_items(connection: duckdb.DuckDBPyConnection):
    table_name = "order_items"
    csv_path = "dataset/olist_order_items_dataset.csv"

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
    return handle_table(connection, table_name, sql_create, csv_path)


def handle_order_payments(connection: duckdb.DuckDBPyConnection):
    table_name = "order_payments"
    csv_path = "dataset/olist_order_payments_dataset.csv"
    sql_create = f"""CREATE TABLE IF NOT EXISTS {table_name} (
        order_id VARCHAR(100) UNIQUE,
        payment_sequential INT,
        payment_type VARCHAR(50),
        payment_installments INT,
        payment_value FLOAT
    )
    """
    return handle_table(connection, table_name, sql_create, csv_path)


def handle_order_reviews(connection: duckdb.DuckDBPyConnection):
    table_name = "order_reviews"
    csv_path = "dataset/olist_order_reviews_dataset.csv"
    sql_create = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            review_id VARCHAR(100) PRIMARY KEY,
            order_id VARCHAR(100),
            review_score INT,
            review_comment_title VARCHAR(255),
            review_comment_message VARCHAR(5000) NULL,
            review_creation_date TIMESTAMP,
            review_answer_timestamp TIMESTAMP
        )
    """
    return handle_table(connection, table_name, sql_create, csv_path)


def handle_orders(connection: duckdb.DuckDBPyConnection):
    table_name = "orders"
    csv_path = "dataset/olist_orders_dataset.csv"
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
    return handle_table(connection, table_name, sql_create, csv_path)


def handle_products(connection: duckdb.DuckDBPyConnection):
    table_name = "products"
    csv_path = "dataset/olist_products_dataset.csv"
    sql_create = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            product_id VARCHAR(100) PRIMARY KEY,
            product_category_name VARCHAR(100),
            product_name_lenght INT,
            product_description_lenght INT,
            product_photos_qty INT,
            product_weight_g FLOAT,
            product_length_cm FLOAT,
            product_height_cm FLOAT,
            product_width_cm FLOAT
        )
    """
    return handle_table(connection, table_name, sql_create, csv_path)


def handle_sellers(connection: duckdb.DuckDBPyConnection):
    table_name = "sellers"
    csv_path = "dataset/olist_sellers_dataset.csv"
    sql_create = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            seller_id VARCHAR(100) PRIMARY KEY,
            seller_zip_code_prefix VARCHAR(10),
            seller_city VARCHAR(100),
            seller_state VARCHAR(5)
        )
    """
    return handle_table(connection, table_name, sql_create, csv_path)


def handle_product_category_name_translation(connection: duckdb.DuckDBPyConnection):
    table_name = "product_category_name_translation"
    csv_path = "dataset/product_category_name_translation.csv"
    sql_create = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            product_category_name VARCHAR(100) UNIQUE,
            product_category_name_english VARCHAR(100)
        )
    """
    return handle_table(connection, table_name, sql_create, csv_path)
