import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('loading: ', query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('inserting: ', query)
        
        # extract data from staging_songs
        df = cur.execute("""SELECT * FROM staging_songs""")
        print(df)
        
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('loading staging tables...')
    load_staging_tables(cur, conn)
       
    
    print('inserting into fact and dimension tables...')
    insert_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()
