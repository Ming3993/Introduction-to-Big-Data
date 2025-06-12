from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from dotenv import load_dotenv
import os
import json

# Load environment variables for Kafka connection
load_dotenv()
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

# Flush and close the producer
def kafka_flush():
    try:
        producer.flush()
        print("Kafka producer flushed successfully.")
    except Exception as e:
        print(f"Failed to flush Kafka producer: {e}")
        raise e
    finally:
        producer.close()
        print("Kafka producer closed.")