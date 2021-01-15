import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load events and songs json data from s3 to Redhift
     
    Args
    * cur: the cursor object to execute queries
    * conn: the connection object to commit the change
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data into fact table and dimmension tables from events data and
    songs data staging tables
    
    Args
    * cur: the cursor object to execute queries
    * conn: the connection object to commit the change
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    The main method connects to Redshift database and loads staging tables,
    facts and dimensions tables
    
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()