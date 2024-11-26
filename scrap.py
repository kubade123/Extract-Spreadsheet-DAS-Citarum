import pandas as pd
from dotenv import load_dotenv
import os
import psycopg2
import csv

df = pd.read_csv('Data PPK DAS Citarum tahun 2023.csv')
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
                    indikator = row[0].strip() if row[0] else None
                    target_2025 = float(row[1]) if row[1] else None  # Handle empty values for target
                    capaian = float(row[2]) if row[2] else None      # Handle empty values for capaian
                    satuan = row[3].strip() if row[3] else None
                    persentase_capaian = float(row[4].replace('%', '')) if row[4] else None  # Remove '%' and convert

                    cur.execute(
                        """
                        INSERT INTO citarum.capaian_ppk_2023(
                            indikator, target_2025, capaian, satuan, persentase_capaian, get_at
                        ) 
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        """,
                        (indikator, target_2025, capaian, satuan, persentase_capaian)
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
                CREATE TABLE IF NOT EXISTS citarum.capaian_ppk_2023 (
                    indikator VARCHAR(255),        -- Name of the indicator
                    target_2025 NUMERIC,           -- Target for 2025 (numeric value, allows NULL)
                    capaian NUMERIC,               -- Achievement (numeric value, allows NULL)
                    satuan VARCHAR(50),            -- Unit of measurement (e.g., 'hektar', 'KK', etc.)
                    persentase_capaian NUMERIC,    -- Percentage of achievement
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
    insert_data(conn, 'Data PPK DAS Citarum tahun 2023.csv')
    conn.close()