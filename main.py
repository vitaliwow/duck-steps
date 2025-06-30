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
        handler.create_staging_tables()

        # create analytical tables
        handler.create_analytical_tables()
