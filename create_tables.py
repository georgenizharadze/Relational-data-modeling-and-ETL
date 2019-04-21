import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """Connect to the database

    Connect to the master database, drop the sparkify dataabse
    if it exists and create it anew. Return connection and cursor
    objects for the tables to be created in next steps. 
    """

    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
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
    """Drop tables from a database
    
    Loop through the predefined drop-table queries and execute
    them. 

    Arguments:
    cur -- the database cursor object
    conn -- the database connection object
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create tables in a database

    Loop through the predefined create-table queries and
    execute them. Try-except block provided for visibility 
    into potential exceptions. 

    Arguments:
    cur -- the database cursor object
    conn -- the database connection object    
    """
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print("Tables creation failed")
        print(e)
    finally:
        conn.close()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()