from openai import AzureOpenAI
import pyodbc
import pandas as pd

import os
from dotenv import load_dotenv
from pathlib import Path

import re
from datetime import datetime

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent / "service"))
from database import DatabaseService

script_dir = Path(os.getcwd())
env_path = script_dir / '.env'
load_dotenv(env_path)

class AzureAI:

    def __init__(self):

        self.server = os.getenv('azure_server')
        self.database = os.getenv('azure_database').split(',')
        self.schema = os.getenv('azure_schema')
        self.username = os.getenv('azure_username')
        self.password = os.getenv('azure_password')

        self.sql_file_path = script_dir / "sql-script" / "Schema.sql"

        self.model_name = os.getenv("openai_model_name")

        self.api_key = os.getenv("openai_model_api_key")
        self.api_version = os.getenv("openai_api_version")
        self.azure_endpoint = os.getenv("openai_api_endpoint")

    def database_connection(self):

        conn_str = f"""Driver={{ODBC Driver 17 for SQL Server}}; Server={self.server}; Database={self.database[0]}; Uid={self.username}; Pwd={self.password}; Encrypt=yes; TrustServerCertificate=no; Connection Timeout=30;"""
        conn = pyodbc.connect(conn_str)

        return conn
    
    def ai_response(self, user_prompt):

        with open(self.sql_file_path, 'r', encoding='utf-8') as file:
            schema_query = file.read()

        conn = self.database_connection()

        schema_df = pd.read_sql_query(schema_query, conn)
        schema_df = schema_df[schema_df['TABLE_CATALOG'] == f'{self.database[0]}']
        schema_df = schema_df[schema_df['TABLE_SCHEMA'] == f'{self.schema}']

        schema_text = ""
        for (schema, table), group in schema_df.groupby(['TABLE_SCHEMA', 'TABLE_NAME']):
            columns = ", ".join(f"{row.COLUMN_NAME} ({row.DATA_TYPE})" for _, row in group.iterrows())
            schema_text += f"Table {schema}.{table}: {columns}\n"

        user_prompt = user_prompt

        model_name = self.model_name

        client = AzureOpenAI(
            api_key=self.api_key,
            api_version=self.api_version,
            azure_endpoint=self.azure_endpoint,
        )

        system_message = f"""
        You are a helpful assistant that writes MSSQL2025 Server queries with correct syntax for Microsoft SQL Server 2025.
        Here is the database schema:

        {schema_text}
        """

        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": f"Write a SQL Server query for this: {user_prompt}"}
            ]
        )

        return response.choices[0].message.content
    
    def create_sql_script(self, user_prompt, save_sql=False):

        def find_latest_script_number(folder_path):
            folder = Path(folder_path)
            max_number = 0

            # Pattern to match filenames like Script-<number>.sql
            pattern = re.compile(r"Script-(\d+)\.sql", re.IGNORECASE)

            for file in folder.glob("Script-*.sql"):
                match = pattern.match(file.name)
                if match:
                    number = int(match.group(1))
                    if number > max_number:
                        max_number = number

            return max_number

        generated_sql = self.ai_response(user_prompt=user_prompt)

        # Extract just the SQL code between ```sql and ```
        sql_code_blocks = re.findall(r"```sql\s*(.*?)```", generated_sql, re.DOTALL)
        description_blocks = re.split(r"```(?:sql)?[\s\S]*?```", generated_sql)

        if sql_code_blocks:
            generated_sql = sql_code_blocks[0].strip()
            generated_description = description_blocks[1].strip()
        else:
            generated_sql = generated_sql.strip()  # fallback
            generated_description = generated_sql.strip()  # fallback

        query_string = f"""{generated_sql}"""

        conn = self.database_connection()

        try:
            df = pd.read_sql_query(query_string, conn)
            # print(f"Query Results: {df}")
        except Exception as e:
            print("Error running the query:", e)

        Author = "AI Generated SQL Query"

        day = datetime.now().day
        month = datetime.now().month
        year = datetime.now().year
        date_obj = datetime(year, month, day)
        create_date = date_obj.strftime("%d/%m/%Y")

        description = generated_description

        header = f"""-- =============================================
-- Author:		{Author}
-- Create date: {create_date}
-- Description:	{description}
-- =============================================
        """

        folder_path = script_dir / "sql-script"
        latest_number = find_latest_script_number(folder_path)
        # print("Latest script number:", latest_number)

        output_path = script_dir / "sql-script" / f"Script-{latest_number+1}.sql"

        # Make sure the directory exists, create if not
        output_path.parent.mkdir(parents=True, exist_ok=True)

        full_sql = header + "\n" + query_string

        if save_sql:
            # Write to file
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(full_sql)
        else:
            pass

        if save_sql == True:
            return f"\nGenerated SQL:\n, {generated_sql}\n\n Query Results: {df}\n SQL file saved to: {output_path}"
        else:
            return f"\nGenerated SQL:\n, {generated_sql}\n\n Query Results: {df}"
    
def main():
    ai = AzureAI()

    # Example: prompt user input and create a script    
    prompt = input("Enter your SQL request: ")

    save_sql = input("Do you want to save the SQL script? (yes/no): ").strip().lower()

    if save_sql == 'yes':
        save_sql = True
    else:
        save_sql = False

    result = ai.create_sql_script(user_prompt=prompt, save_sql=save_sql)
    print(result)

if __name__ == "__main__":
    main()
