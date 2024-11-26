import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os
import psycopg2
import csv

# df = pd.read_csv('Data PPK DAS Citarum tahun 2023.csv')
# print(df.head(10))
csv_file = 'Data PPK DAS Citarum tahun 2023.csv'
sheet_id = '1ihz4Kf7caGd0Gsyceze-7Rr6t9mmFDPUfSbbnzvGKxM'
sheet_name = 'Sheet1'
url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}'
df = pd.read_csv(url)   


def connect_to_db_staging():
    load_dotenv(dotenv_path='.env.staging')
    # """Connect to the PostgreSQL database and return the connection object."""
    # try:
    #     conn = psycopg2.connect(
    #         host=os.getenv('HOST'),
    #         port=os.getenv('PORT'),
    #         database=os.getenv('DATABASE'),
    #         user=os.getenv('DB_USER'),
    #         password=os.getenv('DB_PASS')
    #     )
    #     print("Connection to database established successfully.")
    #     return conn
    # except Exception as e:
    #     print(f"Failed to connect to the database: {e}")
    #     return None
    connection_url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
        f"@{os.getenv('HOST')}:{os.getenv('PORT')}/{os.getenv('DATABASE')}"
    )   
    
    # Create SQLAlchemy engine
    try:
        engine = create_engine(connection_url)
        print("SQLAlchemy engine created successfully.")
        return engine
    except Exception as e:
        print(f"Failed to create SQLAlchemy engine: {e}")
        return None
    
def connect_to_db_production():
    load_dotenv(dotenv_path='.env.production')
    # """Connect to the PostgreSQL database and return the connection object."""
    # try:
    #     conn = psycopg2.connect(
    #         host=os.getenv('HOST'),
    #         port=os.getenv('PORT'),
    #         database=os.getenv('DATABASE'),
    #         user=os.getenv('DB_USER'),
    #         password=os.getenv('DB_PASS')
    #     )
    #     print("Connection to database established successfully.")
    #     return conn
    # except Exception as e:
    #     print(f"Failed to connect to the database: {e}")
    #     return None
    connection_url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
        f"@{os.getenv('HOST')}:{os.getenv('PORT')}/{os.getenv('DATABASE')}"
    )
    
    # Create SQLAlchemy engine
    try:
        engine = create_engine(connection_url)
        print("SQLAlchemy engine created successfully.")
        return engine
    except Exception as e:
        print(f"Failed to create SQLAlchemy engine: {e}")
        return None
    
def insert_data(engine, df):
    # """Insert data from the CSV file into the PostgreSQL database under the 'raw' schema."""
    # try:
    #     with conn.cursor() as cur:
    #         with open(csv_file, mode='r') as file:
    #             reader = csv.reader(file)
    #             next(reader)  # Skip the header row

    #             # Iterate over the rows in the CSV file
    #             for row in reader:
    #                 # Clean and handle empty strings
    #                 indikator = row[0].strip() if row[0] else None
    #                 target_2025 = float(row[1]) if row[1] else None  # Handle empty values for target
    #                 capaian = float(row[2]) if row[2] else None      # Handle empty values for capaian
    #                 satuan = row[3].strip() if row[3] else None
    #                 persentase_capaian = float(row[4].replace('%', '')) if row[4] else None  # Remove '%' and convert

    #                 cur.execute(
    #                     """
    #                     INSERT INTO citarum.capaian_ppk_2023(
    #                         indikator, target_2025, capaian, satuan, persentase_capaian, get_at
    #                     ) 
    #                     VALUES (%s, %s, %s, %s, %s, NOW())
    #                     """,
    #                     (indikator, target_2025, capaian, satuan, persentase_capaian)
    #                 )

    #         conn.commit()
    #         print("Data inserted successfully.")
    # except Exception as e:
    #     print(f"Failed to insert data: {e}")
    
    """Insert data from the CSV file into the PostgreSQL database under the 'citarum' schema."""
    

    # Data cleaning and transformation
    
    df = df.set_axis(['indikator', 'target_2025', 'capaian', 'satuan', 'persentase_capaian'], axis=1)
    df['indikator'] = df['indikator'].apply(lambda x: x.strip() if pd.notnull(x) else None)
    df['target_2025'] = pd.to_numeric(df['target_2025'], errors='coerce')  # Handle conversion to float
    df['capaian'] = pd.to_numeric(df['capaian'], errors='coerce')
    df['satuan'] = df['satuan'].apply(lambda x: x.strip() if pd.notnull(x) else None)
    df['persentase_capaian'] = df['persentase_capaian'].str.replace('%', '').astype(float)

    # Insert data into the table using the DataFrame's `to_sql` method
    try:
        df.to_sql(
            'capaian_ppk_2023_ver2', 
            engine, 
            schema='citarum', 
            if_exists='append', 
            index=False
        )
        print("Data inserted successfully.")
    except Exception as e:
        print(f"Failed to insert data: {e}")

def create_tables(engine):
    """Create the table in the PostgreSQL database under the 'citarum' schema if it does not exist."""
    try:
        with engine.connect() as conn:
            # Create the table if it does not exist
            conn.execute(text("""
    CREATE TABLE IF NOT EXISTS citarum.capaian_ppk_2023_ver2 (
        indikator VARCHAR(255),        -- Name of the indicator
        target_2025 NUMERIC,           -- Target for 2025 (numeric value, allows NULL)
        capaian NUMERIC,               -- Achievement (numeric value, allows NULL)
        satuan VARCHAR(50),            -- Unit of measurement (e.g., 'hektar', 'KK', etc.)
        persentase_capaian NUMERIC,    -- Percentage of achievement
        tahun INTEGER,                 -- Year
        get_at TIMESTAMP DEFAULT NOW() -- Timestamp of data insertion
    )
    """))
            print("Table created successfully or already exists.")
            conn.commit()
    except Exception as e:
        print(f"Failed to create table: {e}")


def test_db_connection(engine):
    try:
        with engine.connect() as conn:
            # Use SQLAlchemy's `text` to wrap the query string
            result = conn.execute(text("SELECT * from citarum.capaian_ppk_2023_backup"))
            print("Connection successful. Test query result:", result.fetchone()[0])
    except Exception as e:
        print(f"Database connection test failed: {e}")

# conn = connect_to_db_staging()
# conn = connect_to_db_production()
# engine = connect_to_db_production()
# engine = connect_to_db_staging()

engine = connect_to_db_staging()
# if engine:
#     test_db_connection(engine)
# else:
#     print("Failed to create the database engine.")

# if conn:
#     create_tables(conn)
#     insert_data(conn, 'Data PPK DAS Citarum tahun 2023.csv')
#     conn.close()

# df['indikator'] = df['indikator'].apply(lambda x: x.strip() if pd.notnull(x) else None)
print(df.head())
if engine:
    create_tables(engine)
    insert_data(engine, df) 