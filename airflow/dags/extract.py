import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('COINGECKO_API_KEY')

if not API_KEY:
    raise EnvironmentError ('COINGECKO_API_KEY is not set!')

def fetch_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": 'bitcoin,dogecoin',
        'vs_currencies': 'eur',
        'include_24hr_change': 'true'
    }

    headers = {
        'x-cg-demo-api-key': API_KEY
    }

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()

    data = response.json()

    rows = []
    for coin, values in data.items():
        rows.append({
            'coin': coin,
            'price_eur': values['eur'],
            'change': values['eur_24h_change'],
            'fetched_at': pd.Timestamp.now(),
        })
    
    return pd.DataFrame(rows)

if __name__ == "__main__":
    df = fetch_prices()
    print(df)