import pandas as pd
from dotenv import load_dotenv
import os
import psycopg2
import csv

df = pd.read_csv('Indeks Kualitas Air Citarum.csv')
# print(df.head(10))
load_dotenv(dotenv_path='.env')

def connect_to_db():
    """Connect to the PostgreSQL database and return the connection object."""
    try:
        conn = psycopg2.connect(
            host=os.getenv('HOST'),
            port=os.getenv('PORT'),
            database=os.getenv('DATABASE'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS')
        )
        print("Connection to database established successfully.")
        return conn
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return None

def insert_data(conn, csv_file):
    """Insert data from the CSV file into the PostgreSQL database under the 'raw' schema."""
    try:
        with conn.cursor() as cur:
            with open(csv_file, mode='r') as file:
                reader = csv.reader(file)
                next(reader)  # Skip the header row

                # Iterate over the rows in the CSV file
                for row in reader:
                    # Clean and handle empty strings
                    tahun = int(row[0])  # Convert 'Tahun' to integer
                    target = float(row[1]) if row[1] != '-' else None  # Handle empty or '-' values for target
                    realisasi = float(row[2])  # Convert 'Realisasi' to float

                    cur.execute(
                        """
                        INSERT INTO citarum.kualitas_air_citarum(
                            tahun, target, realisasi, get_at
                        ) 
                        VALUES (%s, %s, %s, NOW())
                        """,
                        (tahun, target, realisasi)
                    )

            conn.commit()
            print("Data inserted successfully.")
    except Exception as e:
        print(f"Failed to insert data: {e}")

def create_tables(conn):
    """Create the table in the PostgreSQL database under the 'raw' schema if it does not exist."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS citarum.kualitas_air_citarum (
                    tahun INTEGER,                -- Year of data
                    target NUMERIC,               -- Target for the year (numeric, allows NULL)
                    realisasi NUMERIC,            -- Actual realization (numeric)
                    get_at TIMESTAMP DEFAULT NOW() -- Timestamp of data insertion
                )
            """)
            conn.commit()
            print("Table created successfully or already exists.")
    except Exception as e:
        print(f"Failed to create table: {e}")

conn = connect_to_db()
if conn:
    create_tables(conn)
    insert_data(conn, 'Indeks Kualitas Air Citarum.csv')
    conn.close()
