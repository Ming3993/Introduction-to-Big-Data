from dotenv import load_dotenv
from flask import Flask
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
import pendulum
import requests
import time
import threading
import os
import json

app = Flask(__name__)
load_dotenv()
@app.route('/')
def announce_server_running():
    return 'Extract server is running!'

# Load data crawl environment variables
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

# Load environment variables for Kafka connection
KAFKA_HOST = os.getenv('KAFKA_HOST', 'localhost')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_CONNECTION_TIMEOUT = int(os.getenv('KAFKA_CONNECTION_TIMEOUT'))
KAFKA_MAX_RETRIES = int(os.getenv('KAFKA_MAX_RETRIES'))

# Create a producer
producer = KafkaProducer(
    bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send to Kafka
def send_to_kafka(json_value):
    try:
        producer.send(KAFKA_TOPIC, key="btcusdt", value=json_value)
    except Exception as e:
        print(f"Kafka send failed: {e}")
        raise e

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
    producer.flush()

# Check for kafka connection
def check_kafka_connection():
    num_retries = 0
    while num_retries < KAFKA_MAX_RETRIES:
        try:
            print(f"Attempting to connect to Kafka at {KAFKA_HOST}:{KAFKA_PORT}...")
            admin_client = KafkaAdminClient(
                bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}", 
                request_timeout_ms=KAFKA_CONNECTION_TIMEOUT,
                client_id='extract_service_test_connection'
            )

            admin_client.list_topics() # Test connection by listing topics

            admin_client.close()
            print("Kafka connection successful.")
            return True
        except Exception as e:
            print(f"Kafka connection failed: {e}")
            num_retries += 1
    return False

APP_PORT = int(os.getenv('APP_PORT', 5000))
if __name__ == '__main__':
    # Check Kafka connection
    if not check_kafka_connection():
        print("Could not connect to Kafka. Exiting.")
        exit(1)

    # Scrape BTC/USDT value in a separate thread
    thread = threading.Thread(target=get_btcusdt_value, daemon=True)
    thread.start()

    # Start Flask server
    app.run(host='0.0.0.0', port=APP_PORT, debug=False)