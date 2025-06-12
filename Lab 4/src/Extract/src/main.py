from crawler import get_btcusdt_value
from kafka_publisher import check_kafka_connection
from dotenv import load_dotenv
from flask import Flask
import threading
import os

app = Flask(__name__)

@app.route('/')
def announce_server_running():
    return 'Extract server is running!'

# Load environment variables
load_dotenv()
APP_PORT = int(os.getenv('APP_PORT', 5000))

# Main function to start the Flask server and scrape BTC/USDT value
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