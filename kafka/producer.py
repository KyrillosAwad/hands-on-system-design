import json
import csv
import time
import configparser
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Load configuration from INI file
config = configparser.ConfigParser()
config.read("config.ini")

# Configure logging
log_level = getattr(logging, config.get("logging", "level", fallback="INFO"))
log_format = config.get(
    "logging", "format", fallback="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logging.basicConfig(level=log_level, format=log_format)
logger = logging.getLogger(__name__)


class AdClickProducer:
    """
    Kafka producer for ad click data
    """

    def __init__(self, bootstrap_servers=None, topic=None):
        """
        Initialize the Kafka producer

        Args:
            bootstrap_servers (str): Kafka broker addresses (optional, uses config if not provided)
            topic (str): Kafka topic to publish to (optional, uses config if not provided)
        """
        # Load configuration from INI file
        self.bootstrap_servers = bootstrap_servers or config.get("kafka", "bootstrap_servers")
        self.topic = topic or config.get("kafka", "topic")

        # Create Kafka producer with JSON serialization
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8") if k else None,
            # Configuration for reliability from INI file
            acks=config.get("producer", "acks"),
            retries=config.getint("producer", "retries"),
            batch_size=config.getint("producer", "batch_size"),
            linger_ms=config.getint("producer", "linger_ms"),
            buffer_memory=config.getint("producer", "buffer_memory"),
            compression_type=config.get("producer", "compression_type"),
        )

        logger.info(
            f"AdClickProducer initialized with servers: {self.bootstrap_servers}, topic: {self.topic}"
        )

    def send_ad_click(self, ad_click_data, key=None):
        """
        Send a single ad click event to Kafka

        Args:
            ad_click_data (dict): Ad click data to send
            key (str): Optional message key for partitioning
        """
        try:
            # Add processing timestamp
            ad_click_data["processed_at"] = datetime.now().isoformat()

            # Send to Kafka
            future = self.producer.send(
                self.topic, value=ad_click_data, key=key or ad_click_data.get("id")
            )

            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)

            logger.debug(
                f"Message sent to topic {record_metadata.topic} "
                f"partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )

            return True

        except KafkaError as e:
            logger.error(f"Failed to send message: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False

    def send_batch_from_csv(self, csv_file_path=None, delay_seconds=None):
        """
        Send ad click data from CSV file to Kafka

        Args:
            csv_file_path (str): Path to CSV file with ad click data (optional, uses config if not provided)
            delay_seconds (float): Delay between messages to simulate real-time (optional, uses config if not provided)
        """
        # Use config values if not provided
        csv_file_path = csv_file_path or config.get("data_generation", "csv_file_path")
        delay_seconds = delay_seconds or config.getfloat("simulation", "batch_delay")

        try:
            with open(csv_file_path, "r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)

                sent_count = 0
                failed_count = 0

                logger.info(f"Starting to send data from {csv_file_path}")

                for row in reader:
                    # Convert string values to appropriate types
                    processed_row = self._process_csv_row(row)

                    # Send to Kafka
                    if self.send_ad_click(processed_row):
                        sent_count += 1
                        if sent_count % 10 == 0:
                            logger.info(f"Sent {sent_count} messages")
                    else:
                        failed_count += 1

                    # Add delay to simulate real-time streaming
                    if delay_seconds > 0:
                        time.sleep(delay_seconds)

                logger.info(
                    f"Batch processing complete. Sent: {sent_count}, Failed: {failed_count}"
                )

        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_file_path}")
        except Exception as e:
            logger.error(f"Error processing CSV file: {e}")

    def _process_csv_row(self, row):
        """
        Process CSV row and convert data types

        Args:
            row (dict): CSV row data

        Returns:
            dict: Processed row with correct data types
        """
        processed = {}

        for key, value in row.items():
            # Convert numeric fields
            if key in ["click_position_x", "click_position_y", "time_on_page"]:
                processed[key] = int(value) if value else 0
            elif key in ["cost_per_click", "conversion_value"]:
                processed[key] = float(value) if value else 0.0
            elif key == "is_conversion":
                processed[key] = value.lower() == "true"
            else:
                processed[key] = value

        return processed

    def close(self):
        """Close the producer and flush any remaining messages"""
        try:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing producer: {e}")


def main():
    """Main function to demonstrate the producer"""

    # Initialize producer (uses INI config)
    producer = AdClickProducer()

    try:
        print("Starting Kafka Producer - Sending data from CSV file...")
        producer.send_batch_from_csv()
        print("Producer finished sending all CSV data.")

    except KeyboardInterrupt:
        print("\nStopping producer...")
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
