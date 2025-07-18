# Database ingestion using Pyscopg2
# Created by Daniyal Qasim

# Packages
import psycopg2
# directory
from src.config.user_config import db_config

# Main code
def connect():
    """Establishes the PostgreSQL connection using psycopg2"""
    try:
        conn = psycopg2.connect(**db_config)
        print("Connected to the database")
        return conn
    except Exception as e:
        print("Failed to connect:", e)
        return None

def create_table(conn):
    """Creates a sample employees table"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                role VARCHAR(100),
                salary NUMERIC
            )
        """)
        conn.commit()
        print("🛠️ Table 'employees' created or already exists.")

def insert_data(conn):
    """Inserts sample data into the employees table"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO employees (name, role, salary)
            VALUES 
                ('Alice Smith', 'Data Analyst', 72000),
                ('Bob Johnson', 'Software Engineer', 95000),
                ('Charlie Lee', 'BI Developer', 85000)
        """)
        conn.commit()
        print("Sample data inserted.")

def fetch_data(conn):
    """Fetches and prints all records from the employees table"""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM employees")
        rows = cur.fetchall()
        print("📊 Employees:")
        for row in rows:
            print(row)

def main():
    conn = connect()
    if conn:
        create_table(conn)
        insert_data(conn)
        fetch_data(conn)
        conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    main()
