import json
import time
import configparser
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition
import logging
import signal
import sys
from collections import defaultdict

# Load configuration from INI file
config = configparser.ConfigParser()
config.read("config.ini")

# Configure logging
log_level = getattr(logging, config.get("logging", "level"))
log_format = config.get("logging", "format")
logging.basicConfig(level=log_level, format=log_format)
logger = logging.getLogger(__name__)


class Statistics:
    """
    Statistics tracking class for ad click data processing
    """

    def __init__(self):
        """Initialize statistics tracking"""
        self.total_messages = 0
        self.messages_per_second = 0
        self.start_time = None
        self.last_stats_time = None
        self.last_message_count = 0
        self.campaigns = defaultdict(int)
        self.devices = defaultdict(int)
        self.conversions = 0
        self.total_revenue = 0.0
        self.total_cost = 0.0

    def increment_message(self):
        """Increment total message count"""
        self.total_messages += 1

    def add_campaign(self, campaign_id):
        """Add a campaign click"""
        self.campaigns[campaign_id or "unknown"] += 1

    def add_device(self, device_type):
        """Add a device type click"""
        self.devices[device_type or "unknown"] += 1

    def add_conversion(self, conversion_value=0.0):
        """Add a conversion with optional value"""
        self.conversions += 1
        self.total_revenue += conversion_value

    def add_cost(self, cost_per_click=0.0):
        """Add cost per click to total cost"""
        self.total_cost += cost_per_click

    def get_conversion_rate(self):
        """Get conversion rate as percentage"""
        if self.total_messages > 0:
            return (self.conversions / self.total_messages) * 100
        return 0.0

    def get_roi(self):
        """Get ROI as percentage"""
        if self.total_cost > 0:
            return ((self.total_revenue - self.total_cost) / self.total_cost) * 100
        return 0.0

    def get_average_mps(self):
        """Get average messages per second"""
        if self.start_time is None:
            return 0.0
        total_time = time.time() - self.start_time
        return self.total_messages / total_time if total_time > 0 else 0.0

    def update_mps(self):
        """Update messages per second calculation"""
        current_time = time.time()

        if self.start_time is None:
            self.start_time = current_time
            self.last_stats_time = current_time
            return

        if self.last_stats_time is not None:
            time_diff = current_time - self.last_stats_time
            if time_diff >= 1.0:  # Update every second
                message_diff = self.total_messages - self.last_message_count
                self.messages_per_second = message_diff / time_diff
                self.last_stats_time = current_time
                self.last_message_count = self.total_messages

    def get_top_campaigns(self, limit=5):
        """Get top campaigns by click count"""
        return sorted(self.campaigns.items(), key=lambda x: x[1], reverse=True)[:limit]

    def get_device_breakdown(self):
        """Get device breakdown sorted by count"""
        return sorted(self.devices.items(), key=lambda x: x[1], reverse=True)


class AdClickConsumer:
    """
    Kafka consumer for ad click data processing
    """

    def __init__(self, bootstrap_servers=None, topic=None, group_id=None):
        """
        Initialize the Kafka consumer

        Args:
            bootstrap_servers (str): Kafka broker addresses (optional, uses config if not provided)
            topic (str): Kafka topic to consume from (optional, uses config if not provided)
            group_id (str): Consumer group ID (optional, defaults to 'ad-click-analytics')
        """
        # Load configuration from INI file
        self.bootstrap_servers = bootstrap_servers or config.get("kafka", "bootstrap_servers")
        self.topic = topic or config.get("kafka", "topic")
        self.group_id = group_id or config.get("consumer", "group_id")
        self.auto_offset_reset = config.get("consumer", "auto_offset_reset")

        # Statistics tracking
        self.stats = Statistics()

        # Create Kafka consumer
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            # Consumer configuration
            auto_offset_reset=self.auto_offset_reset,  # Configurable offset reset strategy
            enable_auto_commit=True,  # Let Kafka handle offset management automatically
            auto_commit_interval_ms=1000,  # Commit offsets every 1 second
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_records=500,
            fetch_min_bytes=1,
            fetch_max_wait_ms=500,
        )

        # Graceful shutdown handler
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.running = True
        logger.info(f"AdClickConsumer initialized")
        logger.info(f"Bootstrap servers: {self.bootstrap_servers}")
        logger.info(f"Topic: {self.topic}")
        logger.info(f"Group ID: {self.group_id}")
        logger.info(f"Auto offset reset: {self.auto_offset_reset}")

    def _signal_handler(self, signum, frame):
        """Handle graceful shutdown"""
        logger.info(f"Received signal {signum}. Shutting down gracefully...")
        self.running = False

    def process_message(self, message):
        """
        Process a single ad click message

        Args:
            message: Kafka message containing ad click data
        """
        try:
            # Parse message data
            data = message.value

            logger.info(f"Processing message: {data.get('id', 'unknown')}")

            # Update statistics
            self.stats.increment_message()
            self.stats.add_campaign(data.get("campaign_id"))
            self.stats.add_device(data.get("device_type"))

            # Track conversions and revenue
            if data.get("is_conversion", False):
                self.stats.add_conversion(data.get("conversion_value", 0.0))

            self.stats.add_cost(data.get("cost_per_click", 0.0))

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            logger.error(f"Message content: {message.value}")

    def print_statistics(self):
        """Print current consumption statistics"""
        current_time = time.time()

        if self.stats.start_time is None:
            self.stats.start_time = current_time
            self.stats.last_stats_time = current_time
            return

        # Calculate messages per second
        if self.stats.last_stats_time is not None:
            time_diff = current_time - self.stats.last_stats_time
            if time_diff >= 1.0:  # Print stats every second
                message_diff = self.stats.total_messages - self.stats.last_message_count
                self.stats.messages_per_second = message_diff / time_diff

                # Calculate totals
                total_time = current_time - self.stats.start_time
                avg_mps = self.stats.get_average_mps()

                # Calculate conversion rate
                conversion_rate = self.stats.get_conversion_rate()

                # Calculate ROI
                roi = self.stats.get_roi()

                print(f"\n{'='*80}")
                print(f"📊 AD CLICK ANALYTICS - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'='*80}")
                print(f"📈 Total Messages: {self.stats.total_messages:,}")
                print(f"⚡ Current Rate: {self.stats.messages_per_second:.1f} msg/sec")
                print(f"📊 Average Rate: {avg_mps:.1f} msg/sec")
                print(f"💰 Conversions: {self.stats.conversions:,} ({conversion_rate:.1f}%)")
                print(f"💵 Total Revenue: ${self.stats.total_revenue:,.2f}")
                print(f"💸 Total Cost: ${self.stats.total_cost:,.2f}")
                print(f"📈 ROI: {roi:.1f}%")

                # Top campaigns
                if self.stats.campaigns:
                    print(f"\n🎯 Top Campaigns:")
                    top_campaigns = self.stats.get_top_campaigns()
                    for campaign, count in top_campaigns:
                        percentage = (
                            (count / self.stats.total_messages * 100)
                            if self.stats.total_messages > 0
                            else 0
                        )
                        print(f"   {campaign}: {count:,} clicks ({percentage:.1f}%)")

                # Device breakdown
                if self.stats.devices:
                    print(f"\n📱 Device Types:")
                    for device, count in self.stats.get_device_breakdown():
                        percentage = (
                            (count / self.stats.total_messages * 100)
                            if self.stats.total_messages > 0
                            else 0
                        )
                        print(f"   {device}: {count:,} clicks ({percentage:.1f}%)")

                print(f"{'='*80}")

                # Update tracking variables
                self.stats.last_stats_time = current_time
                self.stats.last_message_count = self.stats.total_messages

    def consume_messages(self, print_stats_interval=15):
        """
        Start consuming messages from Kafka

        Args:
            print_stats_interval (int): Interval in seconds to print statistics
        """
        logger.info("Starting to consume messages...")
        logger.info(f"Subscribed to topic: {self.topic}")

        last_stats_print = time.time()

        try:
            while self.running:
                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=1000)

                if message_batch:
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            if not self.running:
                                break
                            self.process_message(message)

                    # No need to manually commit - Kafka handles it automatically

                # Print statistics periodically
                current_time = time.time()
                if current_time - last_stats_print >= print_stats_interval:
                    self.print_statistics()
                    last_stats_print = current_time

                time.sleep(1)  # Sleep to avoid busy-waiting

        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt, shutting down...")
        except Exception as e:
            logger.error(f"Error in consume_messages: {e}")
        finally:
            self.close()

    def close(self):
        """Close the consumer and cleanup resources"""
        logger.info("Closing consumer...")
        self.running = False

        # No need to manually commit - Kafka auto-commit handles it

        # Print final statistics
        print(f"\n🏁 FINAL STATISTICS")
        print(f"{'='*50}")
        print(f"Total Messages Processed: {self.stats.total_messages:,}")
        print(f"Total Conversions: {self.stats.conversions:,}")
        print(f"Total Revenue: ${self.stats.total_revenue:,.2f}")
        print(f"Total Cost: ${self.stats.total_cost:,.2f}")

        if self.stats.total_cost > 0:
            roi = self.stats.get_roi()
            print(f"Final ROI: {roi:.1f}%")

        try:
            self.consumer.close()
            logger.info("Consumer closed successfully")
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")


def main():
    """Main function to run the consumer"""
    print("🚀 Starting Ad Click Data Consumer")
    print("=" * 50)

    try:
        # Create and start consumer
        consumer = AdClickConsumer()

        print(f"📡 Connecting to Kafka at {consumer.bootstrap_servers}")
        print(f"🎯 Consuming from topic: {consumer.topic}")
        print(f"👥 Consumer group: {consumer.group_id}")
        print(f"🔄 Press Ctrl+C to stop consuming")
        print("=" * 50)

        # Start consuming messages
        consumer.consume_messages(print_stats_interval=5)

    except Exception as e:
        logger.error(f"Error in main: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
