import pandas as pd
import psycopg2
import json
import pymssql
from sqlalchemy import create_engine, text
import ast

import os
from dotenv import load_dotenv
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent / "service"))
from database import DatabaseService

script_dir = Path(os.getcwd())
env_path = script_dir / '.env'
load_dotenv(env_path)

class InsertData:

    def __init__(self):

        self.server = os.getenv('azure_server')
        self.database = os.getenv('azure_database')
        self.schema = os.getenv('azure_schema')
        self.username = os.getenv('azure_username')
        self.password = os.getenv('azure_password')

        self.doi_server = os.getenv('doi_server')
        self.doi_port = os.getenv('doi_port')
        self.doi_username = os.getenv('doi_username')
        self.doi_password = os.getenv('doi_password')
        self.doi_database = os.getenv('doi_database')

    def read_table_to_df(self, query=None):

        conn = DatabaseService().create_connection(database=self.doi_database, lib=None, brand='pg')

        cur = conn.cursor()

        cur.execute(query)

        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=colnames)

        return df
    
    def ibs_intell(self):

        ibs_intell_query = """
            SELECT DISTINCT
                i.* 
            FROM 
                ibs_intell i 
            JOIN
                ibs_intell_vector iv ON i.intell_id = iv.intell_id AND iv.is_active = True
            WHERE 
                (iv.model_embedding = 'wordvector_BAAI' OR iv.model_embedding = 'kornwtp/ConGen-model-XLMR')
                AND i.is_publish = TRUE
                AND i.is_active = TRUE
        """

        df = self.read_table_to_df(query=ibs_intell_query)

        return df

    def ibs_intell_vector(self):

        def pad_vector(vector, target_length=1024):
            if len(vector) < target_length:
                # Pad with zeros
                return vector + [0.0] * (target_length - len(vector))
            elif len(vector) > target_length:
                # Truncate to the desired length
                return vector[:target_length]
            else:
                return vector
            
        def serialize_vector(v):
            return json.dumps([round(float(x), 6) for x in v])  # Optional rounding

        ibs_intell_vector_query = """
            SELECT 
                iv.* 
            FROM 
                ibs_intell i 
            JOIN
                ibs_intell_vector iv ON i.intell_id = iv.intell_id AND iv.is_active = True
            WHERE 
                (iv.model_embedding = 'wordvector_BAAI' OR iv.model_embedding = 'kornwtp/ConGen-model-XLMR')
                AND i.is_publish = TRUE
                AND i.is_active = TRUE
        """

        df = self.read_table_to_df(query=ibs_intell_vector_query)

        df['vector'] = df['vector'].apply(ast.literal_eval)
        df['vector'] = df['vector'].apply(pad_vector)
        df['vector'] = df['vector'].apply(serialize_vector)

        return df

def main():

    df_ibs_intell = InsertData().ibs_intell()
    df_ibs_intell_vector = InsertData().ibs_intell_vector()

    DatabaseService().insert_data(database=InsertData.database[1], df=df_ibs_intell, table_name='ibs_intell')
    DatabaseService().insert_data(database=InsertData.database[1], df=df_ibs_intell_vector, table_name='ibs_intell_vector')

if __name__ == "__main__":
    main()
