from kafka_publisher import send_to_kafka, kafka_flush
from dotenv import load_dotenv
import pendulum
import requests
import time
import os

# Load data crawl environment variables
load_dotenv()
DATA_CRAWL_API_URL = os.getenv('DATA_CRAWL_API_URL')
DATA_CRAWL_TIMEOUT = int(os.getenv('DATA_CRAWL_TIMEOUT'))
DATA_CRAWL_MAX_RETRIES = int(os.getenv('DATA_CRAWL_MAX_RETRIES'))
DATA_CRAWL_INTERVAL = float(os.getenv('DATA_CRAWL_INTERVAL'))

def round_datetime_subseconds(dt, interval_ms):
    ms = dt.microsecond // 1000
    rounded_ms = int(round(ms / interval_ms) * interval_ms)
    if rounded_ms >= 1000:
        dt = dt.add(seconds=1).replace(microsecond=0)
    else:
        dt = dt.replace(microsecond=rounded_ms * 1000)
    return dt

# Function to fetch BTC/USDT value from the API
def get_btcusdt_value():
    num_errors = 0 # Initialize error count
    while num_errors < DATA_CRAWL_MAX_RETRIES: # Loop until max retries reached
        try:
            response = requests.get(DATA_CRAWL_API_URL, timeout=DATA_CRAWL_TIMEOUT) # Send GET request

            # Get timestamp
            now = pendulum.now("UTC")
            rounded_now = round_datetime_subseconds(now, interval_ms=DATA_CRAWL_INTERVAL * 1000)
            rounded_now_iso = rounded_now.format("YYYYMMDD[T]HHmmss.SSS") + "Z"

            # Add timestamp to response
            response_json = response.json()
            response_json["timestamp"] = rounded_now_iso

            print(response_json) 
            if response.status_code == 200:
                print("Data fetched successfully.")
                num_errors = 0

                send_to_kafka(response_json)
            else:
                print(f"Failed to fetch data. Status code: {response.status_code}")
                num_errors += 1
        except Exception as e:
            print(f"An error occurred: {e}")
            num_errors += 1

        time.sleep(DATA_CRAWL_INTERVAL) # Sleep and wait before the next request
    kafka_flush()