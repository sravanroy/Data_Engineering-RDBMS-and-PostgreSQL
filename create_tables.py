import psycopg2

from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
        This function connects to the default database as a superuser first.
        Then it drops and recreates the 'sparkify' database and closes the superuser connection.
        Then eventually it connects to the 'sparkify' database as a  non superuser
        and returns the cursor and connection variables of the connection.
    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """
        This function iterates over all the drop table queries and executes them.
        INPUTS:
        * cur the cursor variable of the database
        * conn the connection variable of the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        This function iterates over all the create table queries and executes them.
        INPUTS:
        * cur the cursor variable of the database
        * conn the connection variable of the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()