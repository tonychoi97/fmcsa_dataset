import pandas as pd
from sqlalchemy import create_engine

import os
import argparse
import zipfile

import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    print('Downloading zip file from url.')
    os.system(f'wget {url} -O output.zip')
    print('Downloading finished.')


    print('Extracting zip file.')
    with zipfile.ZipFile('output.zip', 'r') as zip_ref:
        zip_ref.extractall('/app')
    print('Extraction finished.')

    print('Creating engine for postgres.')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print('Engine is created.')

    print('Reading data into dataframe.')
    print('1st check')
    df_iter = pd.read_csv('FMCSA_CENSUS1_2025Jun.txt', iterator=True, chunksize=100000, encoding='cp1252')
    df = next(df_iter)

    print('2nd check')
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    print('3rd check')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    print('4th check')
    while True:
        try:
            t_start = time()
                
            df = next(df_iter)
            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest FMCSA dataset into Postgres')

    parser.add_argument('--user', required=True, help='Postgres Username')
    parser.add_argument('--password', required=True, help='Postgres Password')
    parser.add_argument('--host', required=True, help='Postgres Host')
    parser.add_argument('--port', required=True, help='Postgres Port')
    parser.add_argument('--db', required=True, help='Postgres Database Name')
    parser.add_argument('--table_name', required=True, help='The name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='URL of the CSV file.')

    args = parser.parse_args()

    main(args)




