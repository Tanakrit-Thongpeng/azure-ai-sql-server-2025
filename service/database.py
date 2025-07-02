import urllib
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
        self.database = os.getenv('azure_database')
        self.schema = os.getenv('azure_schema')
        self.username = os.getenv('azure_username')
        self.password = os.getenv('azure_password')

    def create_connection(self, database):

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

        sqlalchemy_url = f"mssql+pyodbc:///?odbc_connect={conn}"

        return sqlalchemy_url
    
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

        conn_str = self.create_connection(database)

        try:
            # Create the SQLAlchemy engine
            engine = create_engine(conn_str)

            # Establish a connection explicitly using engine.connect()
            with engine.connect() as conn:
                # Start a transaction using conn.begin()
                with conn.begin():

                    # Insert the DataFrame into the SQL Server table
                    df.to_sql(table_name, conn, schema=self.schema, if_exists='append', index=False)

            print("Data inserted successfully into the SQL Server table.")
            # conn = conn.close()
        except Exception as e:
            print(f"An error occurred: {e}")