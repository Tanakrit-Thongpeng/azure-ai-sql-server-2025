import urllib
import psycopg2
from sqlalchemy import create_engine
import pandas as pd

import os
from dotenv import load_dotenv
from pathlib import Path

script_dir = Path(os.getcwd())
env_path = script_dir / '.env'
load_dotenv(env_path)

class DatabaseService():
        
    def __init__(self):

        self.server = os.getenv('azure_server')
        self.database = os.getenv('azure_database').split(',')
        self.schema = os.getenv('azure_schema')
        self.username = os.getenv('azure_username')
        self.password = os.getenv('azure_password')

        self.doi_server = os.getenv('doi_server')
        self.doi_port = os.getenv('doi_port')
        self.doi_username = os.getenv('doi_username')
        self.doi_password = os.getenv('doi_password')
        self.doi_database = os.getenv('doi_database')

    def create_connection(self, database, lib=None, brand='mssql'):

        if brand == 'mssql':

            conn_str = (
                "Driver={ODBC Driver 17 for SQL Server};"
                f"Server={self.server};"
                f"Database={database};"
                f"Uid={self.username};"
                f"Pwd={self.password};"
                "Encrypt=yes;"
                "TrustServerCertificate=yes;"
                "Connection Timeout=30;"
            )
            
            conn = urllib.parse.quote_plus(conn_str)

            if lib == 'pyodbc':
                connection = f"mssql+pyodbc:///?odbc_connect={conn}"
            else:
                connection = f"mssql+pymssql://{os.getenv('azure_username')}:{os.getenv('azure_password')}@{os.getenv('azure_server')}:{os.getenv('azure_port')}/{database}"

        else:
            connection = psycopg2.connect(
                dbname=self.doi_database,
                user=self.doi_username,
                password=self.doi_password,
                host=self.doi_server,
                port=self.doi_port
            )

        return connection
    
    def read_table_to_df(self, conn, schema, table_name, query=None):
        
        try:
            if query == None:
                query = f"SELECT * FROM {schema}.{table_name}"
            else:
                query = query

            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"Error reading table {table_name}: {e}")
            return None 

    def insert_data(self, database, df, table_name):

        conn_str = self.create_connection(database, lib=None, brand='mssql')

        try:
            # Create the SQLAlchemy engine
            engine = create_engine(conn_str)

            # Establish a connection explicitly using engine.connect()
            with engine.connect() as conn:
                # Start a transaction using conn.begin()
                with conn.begin():

                    # Insert the DataFrame into the SQL Server table
                    df.to_sql(table_name, conn, schema=self.schema, if_exists='append', index=False)

            print(f"Data inserted successfully into the SQL Server table: {table_name}")
            # conn = conn.close()
        except Exception as e:
            print(f"An error occurred: {e}")