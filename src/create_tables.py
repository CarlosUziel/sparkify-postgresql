import logging
from typing import Any

import psycopg2

from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """

    # 1. Connect to default database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=studentdb user=student password=student"
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # 2. Create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # 3. Close connection to default database
    conn.close()

    # 4. Connect to sparkify database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur: Any, conn: Any):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.

    - Establishes connection with the sparkify database and gets
    cursor to it.

    - Drops all the tables.

    - Creates all tables needed.

    - Finally, closes the connection.
    """
    cur, conn = create_database()

    try:
        drop_tables(cur, conn)
    except psycopg2.Error as e:
        logging.error(f"Error droping tables: \n{e}")
        cur.close()
        conn.close()
        return

    try:
        create_tables(cur, conn)
    except psycopg2.Error as e:
        logging.error(f"Error creating tables: \n{e}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
