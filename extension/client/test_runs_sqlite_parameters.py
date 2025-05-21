import os
from datetime import datetime, timedelta


TMP_DIR = "/home/tmp_duckdb/"
SOURCE_SQLITE_SCRIPTS = "/home/ubuntu/duckdb/extension/client/sqlite_scripts"
CLIENT_CONFIG = "/home/ubuntu/duckdb/extension/client/"
#SOURCE_SQLITE_SCRIPTS = "/home/ila/Code/duckdb/extension/client/sqlite_scripts"
#CLIENT_CONFIG = "/home/ila/Code/duckdb/extension/client/"

SOURCE_POSTGRES_DSN = os.environ.get(
    "PG_DSN", "dbname=rdda_client user=ubuntu password=test host=ec2-18-159-115-118.eu-central-1.compute.amazonaws.com"
)

# Set reference time for RDDA windows (change this to a fixed datetime if needed)
# REFERENCE_TIME = datetime(2024, 1, 1)
REFERENCE_TIME = datetime.now()
WINDOW_DURATION_HOURS = 24

# Configurable parameters (in decimal format)
DEATH_RATE = 0   # Proportion of active clients that "die" each cycle
LATE_RATE = 0    # Proportion of remaining alive clients that become late
NEW_RATE = 0.2   # Proportion of total active clients that are new

# Simulation parameters
MAX_CLIENTS = 4000 # Maximum number of clients to simulate at once
MAX_CONCURRENT_CLIENTS = 10 # Maximum number of clients to simulate in parallel
CHUNK_SIZE = 100 # Number of clients to process in each chunk
INITIAL_CLIENTS = 2000 # Initial number of clients to simulate
CLIENT_DISPATCH_INTERVAL = 5 # Interval in seconds to wait before dispatching new clients
SLEEP_INTERVAL = 15 # Additional interval in seconds to sleep between dispatches
FLUSH_INTERVAL = 10 # Interval in minutes to flush the database
MAX_RUNS = 5 # Maximum number of runs to simulate

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
    "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
    "Fort Worth", "Columbus", "Charlotte", "San Francisco", "Indianapolis",
    "Seattle", "Denver", "Washington", "Boston", "El Paso", "Nashville",
    "Detroit", "Oklahoma City", "Portland", "Las Vegas", "Memphis",
    "Louisville", "Baltimore", "Milwaukee", "Albuquerque", "Tucson",
    "Fresno", "Sacramento", "Mesa", "Kansas City", "Atlanta", "Omaha",
    "Colorado Springs", "Raleigh", "Miami", "Long Beach", "Virginia Beach",
    "Oakland", "Minneapolis", "Tulsa", "Arlington", "Wichita",
    "Bakersfield", "Aurora", "Tampa", "New Orleans", "Cleveland",
    "Anaheim", "Henderson", "Honolulu", "Riverside", "Santa Ana",
    "Corpus Christi", "Lexington", "Stockton", "St. Louis", "Saint Paul",
    "Cincinnati", "Pittsburgh", "Greensboro", "Anchorage", "Plano",
    "Lincoln", "Orlando", "Irvine", "Newark", "Toledo",
    "Durham", "Chula Vista", "Fort Wayne", "Jersey City", "St. Petersburg",
    "Laredo", "Madison", "Chandler", "Buffalo", "Lubbock",
    "Scottsdale", "Reno", "Glendale", "Gilbert", "Winston–Salem",
    "North Las Vegas", "Norfolk", "Chesapeake", "Garland", "Irving",
    "Hialeah", "Fremont", "Boise", "Richmond", "Baton Rouge"
]
