import psycopg2
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

    def update_row(self, table_name: str, column_name: str, new_value, condition: str, condition_params: tuple = ()):
        query = f"UPDATE {table_name} SET {column_name} = %s WHERE {condition};"
        params = (new_value,) + condition_params
        print(f"Query to be processed:\n\t{query}\nWith params:\n\t{params}")
        self.execute_query(query, params)
        print("Updated data!")

    def select_all(self, table_name: str):
        query = f"SELECT * FROM {table_name};"
        result = self.execute_query(query, fetch=True)
        if result:
            for row in result:
                print(row)
        else:
            print("No data found!")
        return result
        
if __name__ == "__main__":
    db = Postgres()

    # Create Table
    # db.create_table("users")
    table_name = "users"

    # Insert rows
    # table_name = "users"
    # data = {
    #     "name" : "John Doe",
    #     "age" : 24,
    #     "email": "johndoe@example.com"
    # }
    # db.insert_rows(table_name=table_name, data=data)

    # Select All
    db.select_all(table_name=table_name)

    # Update Row
    # Update email for user with id = 1
    # db.update_row(
    #     table_name="users",
    #     column_name="age",
    #     new_value=30,
    #     condition="id = %s",
    #     condition_params=(1,)
    # )
