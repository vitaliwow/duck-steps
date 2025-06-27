import duckdb

from services import HandleOlist


if __name__ == "__main__":
    with duckdb.connect("olist.db") as conn:
        handler = HandleOlist(connection=conn)

        # create transfer csv to duckdb tables
        handler.create_sub_tables()

        # create facts table and fill it
        handler.create_facts_table()

        # create staging layer tables

        # create analytical tables
        # get the most valuable customers
        handler.create_most_valuable_customers()
        # get the most valuable products
        handler.create_three_month_user_purchases()
