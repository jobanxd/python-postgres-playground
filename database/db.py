import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

load_dotenv()

class Postgres:
    def __init__(self):
        self.user=os.getenv("DB_USER")
        self.password=os.getenv("DB_PASSWORD")
        self.db=os.getenv("DB_NAME")
    def get_connection(self):
        try:
            conn = psycopg2.connect(
                host="localhost",
                user=self.user,
                password=self.password,
                database=self.db
            )
            return conn
        except Exception as e:
            print(f"Error on `get_connection()`: {str(e)}")

    def execute_query(self, query: str, params: tuple = None, fetch: bool = False):
        conn = self.get_connection()
        if not conn:
            return None
        result = None
        try: 
            with conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    if fetch:
                        result = cur.fetchall()
        except Exception as e:
            print(f"Error on `execute_query(): {str(e)}")
        finally:
            conn.close()
        return result
    
    # EDIT THE FIELDS BEFORE RUNNING!
    def create_table(self, table_name: str):
        query = """
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                age INT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """.format(table=table_name)
        
        self.execute_query(query)
        print("Successful running create_table()!")

    def insert_rows(self, table_name: str, data: dict):
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s']* len(data))
        values = tuple(data.values())

        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
        self.execute_query(query,values)
        print("Successfull running insert_rows()")

    def update_row(self, table_name: str, column_name: str, new_value, condition: str):
        query = f"UPDATE {table_name} SET {column_name} = {new_value} WHERE {condition};"
        self.execute_query(query)
    
