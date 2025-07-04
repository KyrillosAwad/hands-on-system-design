import csv
import random
import configparser
from datetime import datetime, timedelta
from faker import Faker
import uuid

fake = Faker()

# Load configuration from INI file
config = configparser.ConfigParser()
config.read("config.ini")

# Configuration from INI file
NUM_RECORDS = config.getint("data_generation", "num_records", fallback=1000)
OUTPUT_FILE = config.get("data_generation", "csv_file_path", fallback="ad_click_data.csv")

# Sample data pools
ad_campaigns = [
    "summer_sale_2025",
    "tech_gadgets_promo",
    "fashion_week_special",
    "back_to_school",
    "holiday_deals",
    "fitness_challenge",
    "food_delivery",
    "streaming_service",
    "mobile_app_install",
    "travel_deals",
]

device_types = ["desktop", "mobile", "tablet"]
browser_types = ["chrome", "firefox", "safari", "edge", "opera"]
operating_systems = ["Windows", "macOS", "Linux", "iOS", "Android"]
ad_formats = ["banner", "video", "native", "popup", "interstitial"]


def generate_ad_click_record(id: int) -> dict:
    """Generate a single ad click record"""

    # Generate timestamp within last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    timestamp = fake.date_time_between(start_date=start_date, end_date=end_date).timestamp()

    # Generate user data
    user_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())

    # Generate ad data
    ad_id = f"ad_{random.randint(1000, 9999)}"
    campaign_id = random.choice(ad_campaigns)
    ad_format = random.choice(ad_formats)

    # Generate device/browser data
    device_type = random.choice(device_types)
    browser = random.choice(browser_types)
    os = random.choice(operating_systems)

    # Generate location data
    country = fake.country()

    # Generate engagement data
    click_position_x = random.randint(0, 1920)
    click_position_y = random.randint(0, 1080)
    time_on_page = random.randint(5, 300)  # seconds
    page_url = fake.url()
    referrer_url = fake.url() if random.random() > 0.3 else ""

    # Generate cost data
    cost_per_click = round(random.uniform(0.10, 5.00), 2)

    # Generate conversion data (10% conversion rate)
    is_conversion = random.random() < 0.1
    conversion_value = round(random.uniform(10.00, 500.00), 2) if is_conversion else 0.0

    return {
        "id": id,
        "timestamp": timestamp,
        "user_id": user_id,
        "session_id": session_id,
        "ad_id": ad_id,
        "campaign_id": campaign_id,
        "ad_format": ad_format,
        "device_type": device_type,
        "browser": browser,
        "operating_system": os,
        "country": country,
        "click_position_x": click_position_x,
        "click_position_y": click_position_y,
        "time_on_page": time_on_page,
        "page_url": page_url,
        "referrer_url": referrer_url,
        "cost_per_click": cost_per_click,
        "is_conversion": is_conversion,
        "conversion_value": conversion_value,
    }


def generate_csv_file():
    """Generate CSV file with ad click data"""

    print(f"Generating {NUM_RECORDS} ad click records...")

    # Generate all records
    records = []
    for i in range(NUM_RECORDS):
        if i % 100 == 0:
            print(f"Generated {i} records...")
        records.append(generate_ad_click_record(i))

    # Write to CSV
    fieldnames = records[0].keys()

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"Successfully generated {NUM_RECORDS} records in {OUTPUT_FILE}")


if __name__ == "__main__":
    generate_csv_file()
