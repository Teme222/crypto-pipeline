import os
from sqlalchemy import create_engine
from extract import fetch_prices
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT')
    db = os.getenv('POSTGRES_DB')
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

def load_prices(df=None):
    if df is None: 
        df = fetch_prices()
    engine = get_engine()
    df.to_sql("raw_prices", engine, if_exists='append', index=False)
    print(F"Loaded {len(df)} rows into raw_prices")

if __name__ == "__main__":
    load_prices()