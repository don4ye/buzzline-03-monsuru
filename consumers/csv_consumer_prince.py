"""
csv_consumer_prince.py

Consume JSON messages from a Kafka topic and process them.

Example Kafka message format:
{"timestamp": "2025-01-11T18:15:00Z", "steps": 12500, "heart_rate": 80, "calories_burned": 450.0, "sleep_hours": 7.0, "hydration_liters": 2.5}
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json
from collections import deque

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

# Load environment variables from .env
load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("HEALTH_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("HEALTH_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_stall_threshold() -> float:
    """Fetch message interval from environment or use default."""
    stall_threshold = float(os.getenv("HEALTH_STALL_THRESHOLD_F", 0.2))
    logger.info(f"Max stall temperature range: {stall_threshold} F")
    return stall_threshold


def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("HEALTH_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size


#####################################
# Define a function to detect a stall
#####################################

def detect_stall(rolling_window_deque: deque) -> bool:
    """
    Detect a stall based on the rolling window of health data.

    Args:
        rolling_window_deque (deque): Rolling window of health data.

    Returns:
        bool: True if a stall is detected, False otherwise.
    """
    WINDOW_SIZE: int = get_rolling_window_size()
    if len(rolling_window_deque) < WINDOW_SIZE:
        # We don't have a full deque yet
        logger.debug(f"Rolling window size: {len(rolling_window_deque)}. Waiting for {WINDOW_SIZE}.")
        return False

    # Once the deque is full we can calculate the range
    range_value = max(rolling_window_deque) - min(rolling_window_deque)
    is_stalled: bool = range_value <= get_stall_threshold()
    logger.debug(f"Range: {range_value}°F. Stalled: {is_stalled}")
    return is_stalled


#####################################
# Function to process a single message
#####################################

def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    """
    Process a JSON message and check for stall conditions.

    Args:
        message (str): JSON message received from Kafka.
        rolling_window (deque): Rolling window of health data.
        window_size (int): Size of the rolling window.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)
        timestamp = data.get("timestamp")
        steps = data.get("steps")
        heart_rate = data.get("heart_rate")
        calories_burned = data.get("calories_burned")
        sleep_hours = data.get("sleep_hours")
        hydration_liters = data.get("hydration_liters")

        logger.info(f"Processed JSON message: {data}")

        # Ensure the required fields are present
        if None in (timestamp, steps, heart_rate, calories_burned, sleep_hours, hydration_liters):
            logger.error(f"Invalid message format: {message}")
            return

        # Append the calorie reading to the rolling window
        rolling_window.append(calories_burned)

        # Check for a stall condition
        if detect_stall(rolling_window):
            logger.info(f"STALL DETECTED at {timestamp}: Calories burned stable at {calories_burned} over last {window_size} readings.")

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")


#####################################
# Define main function for this module
#####################################

def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls and processes messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # Fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")

    rolling_window = deque(maxlen=window_size)

    # Create the Kafka consumer using the helpful utility function
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            # Check if message.value is already a string
            if isinstance(message.value, str):
                message_str = message.value  # Already decoded
            else:
                message_str = message.value.decode("utf-8")  # Decode if it's bytes

            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
