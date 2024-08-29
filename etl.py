import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from sql_queries import most_played_songs_query, most_active_users_query, peak_hour_of_usage_query

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def run_example_queries(cur):      
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
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

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
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    run_example_queries(cur)
    conn.close()


if __name__ == "__main__":
    main()
