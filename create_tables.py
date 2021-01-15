import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This method drops tables if they exist
    
    Args
    * cur: the cursor object to execute queries
    * conn: the connection object to commit the change
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This method creates tables if they do not exist
    
    Args
    * cur: the cursor object to execute queries
    * conn: the connection object to commit the change
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    The main method loads the configuration from dwh.cfg file and
    creates staging, fact, and dimensions tables
    
    The following values should be correctly set in the [Cluster section]
    
    HOST:          Redhsift host (endpoint)
    DB_NAME:       Database name
    DB_USER:       Database users
    DB_PASSWORD:   Database password
    DB_PORT:       Database port
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()