# Kafka Ad Click Data Producer

This directory contains a Kafka producer implementation for ad click data processing.

## Files

- `producer.py` - Main Kafka producer implementation
- `generate_ad_click_data.py` - Script to generate sample ad click data
- `consumer.py` - Kafka consumer implementation
- `config.ini` - INI configuration file
- `ad_click_data.csv` - Generated sample data (1000 records)

## Prerequisites

- **Java 8+** - Required for Kafka
- **Python 3.7+** - For running producer/consumer scripts

## Complete Setup Guide

### Step 1: Download and Install Kafka

1. **Download Kafka**

   - Visit https://kafka.apache.org/downloads
   - Download the latest stable version (e.g., `kafka_2.13-3.9.1.tgz`)
   - Extract to `C:\kafka\kafka_2.13-3.9.1\` (or `Z:\kafka_2.13-3.9.1\`)

2. **Verify Java Installation**
   ```cmd
   java -version
   ```

### Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Start Kafka Services

**Important**: Run these commands from the Kafka root directory (not from bin/windows)

#### Terminal 1 - Start ZooKeeper:

```cmd
cd C:\kafka\kafka_2.13-3.9.1
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```

#### Terminal 2 - Start Kafka Server:

```cmd
cd C:\kafka\kafka_2.13-3.9.1
bin\windows\kafka-server-start.bat config\server.properties
```

Wait for both services to start completely. You should see logs indicating they're running.

### Step 4: Create the Required Topic

#### Terminal 3 - Create Topic:

```cmd
cd C:\kafka\kafka_2.13-3.9.1
bin\windows\kafka-topics.bat --create --topic ad-clicks --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

#### Verify Topic Creation:

```cmd
bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

### Step 5: Generate Sample Data

```bash
python generate_ad_click_data.py
```

## Running the System

Now that Kafka is set up and running, you can run the consumer and producer:

### Step 6: Start Consumer (Terminal 4)

```bash
python consumer.py
```

The consumer will:

- Connect to Kafka
- Subscribe to the `ad-clicks` topic
- Wait for incoming messages
- Process and display ad click data

### Step 7: Start Producer (Terminal 5)

```bash
python producer.py
```

The producer will:

- Connect to Kafka
- Send ad click data to the `ad-clicks` topic
- Display progress and statistics

## Troubleshooting

### Common Issues:

1. **"NoBrokersAvailable" Error**

   - Ensure ZooKeeper and Kafka are running
   - Check if services are listening on correct ports (2181 for ZooKeeper, 9092 for Kafka)

2. **"Config file missing" Error**

   - Run commands from Kafka root directory, not from bin/windows
   - Use correct path: `config\zookeeper.properties` not `.\config\zookeeper.properties`

3. **Topic doesn't exist**

   - Create topic using the command in Step 4
   - Verify with `--list` command

4. **Port conflicts**
   - Ensure ports 2181 and 9092 are available
   - Check for other Kafka instances running

### Verification Commands:

```cmd
# List all topics
bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

# Describe topic details
bin\windows\kafka-topics.bat --describe --topic ad-clicks --bootstrap-server localhost:9092

# Check consumer groups
bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list
```

## Ad Click Data Schema

The generated ad click data includes:

- `id` - Unique identifier for the ad click
- `timestamp` - When the click occurred
- `user_id` - Unique user identifier
- `session_id` - User session identifier
- `ad_id` - Advertisement identifier
- `campaign_id` - Marketing campaign identifier
- `ad_format` - Type of ad (banner, video, native, etc.)
- `device_type` - Device used (desktop, mobile, tablet)
- `browser` - Browser type
- `operating_system` - OS type
- `country` - User's country
- `click_position_x/y` - Click coordinates
- `time_on_page` - Time spent on page (seconds)
- `page_url` - URL where ad was clicked
- `referrer_url` - Previous page URL
- `cost_per_click` - Cost per click
- `is_conversion` - Whether click led to conversion
- `conversion_value` - Value of conversion if occurred

## Configuration

The project uses an INI configuration file (`config.ini`) for easy management of settings:


Edit `config.ini` to modify:

- Kafka broker addresses and topic names
- Producer settings
- Consumer settings
- Data generation parameters
- Simulation timing
- Logging levels and formats

## Performance Tuning

### Producer Optimization:

- **batch_size**: Increase for higher throughput
- **linger_ms**: Add small delay to batch more records
- **compression_type**: Use gzip or lz4 for better compression
- **buffer_memory**: Increase for high-volume scenarios

### Consumer Optimization:

- **fetch_min_bytes**: Increase to reduce network calls
- **fetch_max_wait_ms**: Balance between latency and throughput
- **max_poll_records**: Adjust based on processing capacity
