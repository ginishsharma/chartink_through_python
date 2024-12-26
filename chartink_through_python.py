import os
import warnings
import redis
import pandas as pd
from time import sleep
from bs4 import BeautifulSoup
import requests

warnings.filterwarnings("ignore")

# Redis setup
redis_host = "localhost"
redis_port = 6379
redis_db = 0
redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)

Condition = "( {cash} ( latest rsi( 14 ) > 50 and latest ema( latest close , 20 ) > latest ema( latest close , 50 ) and 1 day ago ema( latest close , 20 )<= 1 day ago ema( latest close , 50 ) and latest volume >= latest sma( latest volume , 20 ) and latest count( 3, 1 where latest volume >= latest sma( latest volume , 20 ) ) = 3 and latest close > latest ema( latest close , 20 ) and latest close > latest ema( latest close , 50 ) and market cap > 1000 ) )"

sleeptime = 5

Charting_Link = "https://chartink.com/screener/"
Charting_url = 'https://chartink.com/screener/process'

# Telegram setup
telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_message(message):
    """
    Sends a message to a Telegram chat.
    """
    url = f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage"
    payload = {
        "chat_id": telegram_chat_id,
        "text": message
    }
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("Message sent to Telegram.")
        else:
            print(f"Failed to send message. Response: {response.text}")
    except Exception as e:
        print(f"Error sending Telegram message: {e}")

def get_data_from_chartink(payload):
    payload = {'scan_clause': payload}
    
    with requests.Session() as s:
        r = s.get(Charting_Link)
        soup = BeautifulSoup(r.text, "html.parser")
        csrf = soup.select_one("[name='csrf-token']")['content']
        s.headers['x-csrf-token'] = csrf
        r = s.post(Charting_url, data=payload)

        df = pd.DataFrame()
        for item in r.json()['data']:
            if len(item) > 0:
                df = pd.concat([df, pd.DataFrame.from_dict(item, orient='index').T], ignore_index=True)
        
    return df

def store_data_in_redis(data):
    """
    Stores unique data records in Redis and returns only the new records.
    """
    new_records = []

    for _, row in data.iterrows():
        record_key = f"stock:{row['nsecode']}"  # Assuming 'id' uniquely identifies a record
        if not redis_client.exists(record_key):
            redis_client.hmset(record_key, row.to_dict())
            new_records.append(row.to_dict())

    return pd.DataFrame(new_records)

try:
    while True:
        # Fetch data from Chartink
        data = get_data_from_chartink(Condition)

        if len(data) > 0:
            data = data.sort_values(by='per_chg', ascending=False)

            # Store unique data in Redis and get new records
            new_data = store_data_in_redis(data)

            if not new_data.empty:
                print(f"\n\nNew Data:\n{new_data}")
                 # Send Telegram message for each new record
                for _, record in new_data.iterrows():
                    message = f"New Record Found:\n{record.to_dict()}"
                    send_telegram_message(message)
            else:
                print("\n\nNo new records.")

        sleep(sleeptime)

except KeyboardInterrupt:
    print("Process interrupted by user.")
except Exception as e:
    print(f"Error: {e}")
