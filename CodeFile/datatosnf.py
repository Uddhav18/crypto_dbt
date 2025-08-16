import requests
import json
import snowflake.connector
from datetime import datetime
import time

# ==== Snowflake Connection Details ====
SNOWFLAKE_USER = "YOUR_USER"
SNOWFLAKE_PASSWORD = "YOUR_PASSWORD"
SNOWFLAKE_ACCOUNT = "YOUR_ACCOUNT"  # e.g., abcde-xy12345
SNOWFLAKE_WAREHOUSE = "YOUR_WAREHOUSE"
SNOWFLAKE_DATABASE = "YOUR_DATABASE"
SNOWFLAKE_SCHEMA = "YOUR_SCHEMA"

# ==== Function: Fetch data from CoinGecko API ====
def fetch_crypto_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 5,  # Top 5 coins for demo
        "page": 1,
        "sparkline": "false"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# ==== Function: Insert into Snowflake ====
def insert_into_snowflake(data):
        # Create a connection object
    conn = snowflake.connector.connect(
        user="Uddhav18",
        password="UddhavGavhane@18",
        account="apgazqy-ex48851",
        warehouse="developer",
        database="db_dev",
        schema="landing"
        )
    
    try:
        cur = conn.cursor()
        for record in data:
            json_data = json.dumps(record)  # Convert dict to JSON string
            cur.execute(
                "INSERT INTO RAW_CRYPTO_DATA (data) SELECT PARSE_JSON(%s)",
                (json_data,)
            )
        conn.commit()
        print(f"{len(data)} records inserted successfully.")
    finally:
        cur.close()
        conn.close()

# ==== Main Script ====
if __name__ == "__main__":
    while True:
        try:
            print(f"[{datetime.now()}] Fetching data...")
            crypto_data = fetch_crypto_data()
            insert_into_snowflake(crypto_data)
        except Exception as e:
            print(f"❌ Error: {e}")
        
        # Wait for 1 minute before next fetch
        time.sleep(10)