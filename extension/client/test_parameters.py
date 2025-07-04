import os
from datetime import datetime, timedelta


TMP_DIR = "/home/tmp_duckdb/"
SOURCE_SQLITE_SCRIPTS = "/home/ubuntu/duckdb/extension/client/sqlite_scripts"
CLIENT_CONFIG = "/home/ubuntu/duckdb/extension/client/"
#SOURCE_SQLITE_SCRIPTS = "/home/ila/Code/duckdb/extension/client/sqlite_scripts"
#CLIENT_CONFIG = "/home/ila/Code/duckdb/extension/client/"

SOURCE_POSTGRES_DSN = os.environ.get(
    "PG_DSN", "dbname=sidra_client user=ubuntu password=test host=ec2-52-14-100-33.us-east-2.compute.amazonaws.com"
)

# Set reference time for sidra windows (change this to a fixed datetime if needed)
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
SLEEP_INTERVAL = 5 # Additional interval in seconds to sleep between dispatches
FLUSH_INTERVAL = 10 # Interval in minutes to flush the database
MAX_RUNS = 5 # Maximum number of runs to simulate

# Refresh parameters (note: it takes around 3 min to initialize 2000 clients)
REFRESH = False # Whether to refresh the data in between windows
NUM_CHUNKS = 1 # Number of refreshes per window
UPDATE_WINDOW_EVERY_REFRESH = False # Whether to update the window every refresh (or wait for the next window)

# Parameters for flush and CPU analysis
FLUSH_NAME = "daily_steps_user"
UPDATE_WINDOW_NAME = "sidra_centralized_view_daily_steps_user"
CENTRALIZED = True
RUNS_PER_CLIENT = 1  # Number of data points per client

SKEWED = False # Uniform distribution of clients across cities

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
    "Hialeah", "Fremont", "Boise", "Richmond", "Baton Rouge",
    "Spokane", "Des Moines", "Tacoma", "San Bernardino", "Modesto",
    "Fontana", "Santa Clarita", "Birmingham", "Oxnard", "Fayetteville",
    "Moreno Valley", "Rochester", "Glendale", "Huntington Beach",
    "Salt Lake City", "Grand Rapids", "Amarillo", "Yonkers", "Aurora",
    "Montgomery", "Akron", "Little Rock", "Huntsville", "Augusta",
    "Columbus", "Grand Prairie", "Shreveport", "Overland Park",
    "Tallahassee", "Mobile", "Knoxville", "Worcester", "Tempe",
    "Cape Coral", "Providence", "Fort Lauderdale", "Chattanooga",
    "Oceanside", "Garden Grove", "Rancho Cucamonga", "Santa Rosa",
    "Port St. Lucie", "Ontario", "Vancouver", "Sioux Falls", "Peoria",
    "Springfield", "Pembroke Pines", "Elk Grove", "Salem", "Lancaster",
    "Corona", "Eugene", "Palmdale", "Salinas", "Springfield", "Pasadena",
    "Fort Collins", "Hayward", "Pomona", "Cary", "Rockford",
    "Alexandria", "Escondido", "McKinney", "Kansas City", "Joliet",
    "Sunnyvale", "Torrance", "Bridgeport", "Lakewood", "Hollywood",
    "Paterson", "Naperville", "Syracuse", "Mesquite", "Dayton",
    "Savannah", "Clarksville", "Orange", "Pasadena", "Fullerton",
    "Killeen", "Frisco", "Hampton", "McAllen", "Warren",
    "Bellevue", "West Valley City", "Columbia", "Olathe", "Sterling Heights",
    "New Haven", "Miramar", "Waco", "Thousand Oaks", "Cedar Rapids",
    "Charleston", "Visalia", "Topeka", "Elizabeth"
]

CLUBS = [
    "Club A", "Club B", "Club C", "Club D", "Club E",
    "Club F", "Club G", "Club H", "Club I", "Club J",
    "Club K", "Club L", "Club M", "Club N", "Club O",
    "Club P", "Club Q", "Club R", "Club S", "Club T",
    "Club U", "Club V", "Club W", "Club X", "Club Y",
    "Club Z"
]

