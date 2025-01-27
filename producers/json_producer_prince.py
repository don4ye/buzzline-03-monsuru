import random
import datetime
import json
import time
import pathlib
import sys
import os  # Add this import for os.getenv to work
from dotenv import load_dotenv
from utils.utils_producer import verify_services, create_kafka_producer, create_kafka_topic
from utils.utils_logger import logger

load_dotenv()

def get_kafka_topic() -> str:
    topic = os.getenv("BUZZ_TOPIC", "stock_prices_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    interval = int(os.getenv("BUZZ_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

DATA_FOLDER: pathlib.Path = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

DATA_FILE: pathlib.Path = DATA_FOLDER.joinpath("stock_prices.json")
logger.info(f"Data file: {DATA_FILE}")

def generate_custom_message():
    timestamp = datetime.datetime.now().isoformat()
    symbol = random.choice(["AAPL", "GOOGL", "AMZN", "MSFT"])
    price = round(random.uniform(100, 1500), 2)
    return {
        "timestamp": timestamp,
        "symbol": symbol,
        "price": price
    }

def main():
    logger.info("START producer.")
    verify_services()

    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    producer = create_kafka_producer(value_serializer=lambda x: json.dumps(x).encode("utf-8"))
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        while True:
            message = generate_custom_message()
            producer.send(topic, value=message)
            logger.info(f"Sent message to topic '{topic}': {message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END producer.")

if __name__ == "__main__":
    main()
