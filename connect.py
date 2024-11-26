from dotenv import load_dotenv
from sqlalchemy import create_engine,text
import os

load_dotenv(dotenv_path='.env.staging')

connection_url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
        f"@{os.getenv('HOST')}:{os.getenv('PORT')}/{os.getenv('DATABASE')}"
    )  
try:
    engine = create_engine(connection_url)
    print('SQLAlchemy engine created successfully.')
except:
    print('Failed to create SQLAlchemy engine.')

with engine.connect() as conn:
    result = conn.execute(text('select * from citarum.capaian_ppk_2023'))
    result2 = conn.execute(text('select * from citarum.kualitas_air_citarum'))
    for row in result:
        print("username:", row)
    for row in result2:
        print("alba:", row)

