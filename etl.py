import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from sql_queries import most_played_songs_query, most_active_users_query, peak_hour_of_usage_query

def load_staging_tables(cur, conn):
    print("Starting to load data into staging tables...")
    for query in copy_table_queries:
        print(f"Executing query: {query[:50]}...")  # Print the first 50 characters of the query for brevity
        cur.execute(query)
        conn.commit()
    print("Staging tables loaded successfully.\n")

def insert_tables(cur, conn):
    print("Starting to insert data into final tables...")
    for query in insert_table_queries:
        print(f"Executing query: {query[:50]}...")  # Print the first 50 characters of the query for brevity
        cur.execute(query)
        conn.commit()
    print("Final tables populated successfully.\n")

def run_example_queries(cur):      
    print("Running example queries...\n")
    
    print("Most Played Songs:")
    cur.execute(most_played_songs_query)
    rows = cur.fetchall()
    for row in rows:
        print(row)
    
    print("\nMost Active Users:")
    cur.execute(most_active_users_query)
    rows = cur.fetchall()
    for row in rows:
        print(row)
    
    print("\nPeak Hour of Usage:")
    cur.execute(peak_hour_of_usage_query)
    rows = cur.fetchall()
    for row in rows:
        print(row)

def main():
    print("Starting ETL process...\n")
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Connecting to Redshift cluster...")
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            config.get('CLUSTER', 'HOST'),
            config.get('CLUSTER', 'DB_NAME'),
            config.get('CLUSTER', 'DB_USER'),
            config.get('CLUSTER', 'DB_PASSWORD'),
            config.get('CLUSTER', 'DB_PORT')
        )
    )
    cur = conn.cursor()
    print("Connected to Redshift cluster.\n")
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    run_example_queries(cur)
    
    print("Closing the connection to Redshift cluster.")
    conn.close()
    print("ETL process completed successfully.\n")


if __name__ == "__main__":
    main()
