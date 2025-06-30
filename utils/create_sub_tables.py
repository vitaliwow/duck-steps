import duckdb


def handle_table(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    sql_create: str,
    csv_path: str,
):
    connection.sql(sql_create)
    connection.sql(
        f"INSERT INTO {table_name} SELECT * FROM read_csv('{csv_path}') ON CONFLICT DO NOTHING"
    )
    connection.table(table_name).show()


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
