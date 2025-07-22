import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        host="localhost",
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    cur = conn.cursor()
    cur.execute("SELECT version();")
    db_version = cur.fetchone()
    print(f"PostgreSQL version: {db_version}")
    
    cur.close()
    conn.close()
except Exception as e:
    print(f"Error: {str(e)}")
