import os
import shutil
import argparse
import subprocess
import time
import sqlite3
import random
import csv
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from datetime import datetime, timedelta
import traceback
import socket
import struct

# note: this requires postgres installed, role and database created ("ubuntu" in this case)
# setting postgresql.conf with 100 max clients and listening on all addresses
# also pg_hba.conf with "host    all             all             0.0.0.0/0               md5"
# the database is rdda_client

TMP_DIR = "/home/tmp_duckdb/"
SOURCE_SQLITE_SCRIPTS = "/home/ila/Code/duckdb/extension/client/sqlite_scripts"
CLIENT_CONFIG = "/home/ila/Code/duckdb/extension/client/"

SOURCE_POSTGRES_DSN = os.environ.get(
    "PG_DSN", "dbname=rdda_client user=ubuntu password=test host=ec2-18-159-115-118.eu-central-1.compute.amazonaws.com"
)

CITIES = [
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "Philadelphia",
    "San Antonio",
    "San Diego",
    "Dallas",
    "San Jose",
    "Austin",
    "Jacksonville",
    "Fort Worth",
    "Columbus",
    "Charlotte",
    "San Francisco",
    "Indianapolis",
    "Seattle",
    "Denver",
    "Washington",
    "Boston",
    "El Paso",
    "Nashville",
    "Detroit",
    "Oklahoma City",
    "Portland",
    "Las Vegas",
    "Memphis",
    "Louisville",
    "Baltimore",
    "Milwaukee",
    "Albuquerque",
    "Tucson",
    "Fresno",
    "Sacramento",
    "Mesa",
    "Kansas City",
    "Atlanta",
    "Omaha",
    "Colorado Springs",
    "Raleigh",
    "Miami",
    "Long Beach",
    "Virginia Beach",
    "Oakland",
    "Minneapolis",
    "Tulsa",
    "Arlington",
    "Wichita",
]

# Set reference time for RDDA windows (change this to a fixed datetime if needed)
# REFERENCE_TIME = datetime(2024, 1, 1)
REFERENCE_TIME = datetime.now()
WINDOW_DURATION_HOURS = 4


def get_random_city():
    return random.choice(CITIES)


def format_date(offset_days):
    return (datetime.now() + timedelta(days=offset_days)).strftime('%Y-%m-%d')


def format_time():
    return f"{random.randint(5, 8):02}:{random.randint(0, 59):02}:{random.randint(0, 59):02}"


def parse_client_config(folder_path):
    config_path = os.path.join(folder_path, "client.config")
    config = {}

    try:
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and '=' in line:
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()

    except FileNotFoundError:
        print(f"Config file not found: {config_path}")
        raise
    except Exception as e:
        print(f"Error reading config file {config_path}: {str(e)}")
        traceback.print_exc()
        raise

    return config


def generate_client_info(path):
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                nickname, city, run_count, initialized = f.read().split(",")
                return nickname, city, int(run_count), initialized == 'True'
        except Exception as e:
            print(f"Error reading client info from {path}: {str(e)}")
            traceback.print_exc()
            raise

    nickname = f"user_{random.randint(0, 1500000)}"
    city = get_random_city()
    run_count = 0
    initialized = False
    return nickname, city, run_count, initialized


def save_client_info(path, nickname, city, run_count, initialized):
    try:
        with open(path, 'w') as f:
            f.write(f"{nickname},{city},{run_count},{initialized}")
    except Exception as e:
        print(f"Error saving client info to {path}: {str(e)}")
        traceback.print_exc()


def generate_csv(path, nickname, city, date):
    try:
        if os.path.exists(path):
            os.remove(path)  # ✅ Remove old CSV before generating new one

        with open(path, 'a') as f:
            writer = csv.writer(f)
            for _ in range(random.randint(1, 5)):
                writer.writerow(
                    [
                        nickname,
                        city,
                        date,
                        format_time(),
                        format_time(),
                        random.randint(500, 10500),
                        random.randint(60, 140),
                    ]
                )
    except Exception as e:
        print(f"Error generating CSV at {path}: {str(e)}")
        traceback.print_exc()


def execute_sql_file(conn, db_path, sql_file):
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")

        with open(sql_file, 'r') as f:
            sql_script = f.read()
            try:
                conn.executescript(sql_script)
            except sqlite3.Error as e:
                print(f"Error executing SQL file {sql_file} on database {db_path}: {str(e)}")
                print(f"Failed SQL: {sql_script}")
                traceback.print_exc()
                raise
    except Exception as e:
        print(f"General error processing SQL file {sql_file} for database {db_path}: {str(e)}")
        traceback.print_exc()
        raise


def setup_client_folder(i):
    folder = os.path.join(TMP_DIR, f"client_{i}")
    try:
        os.makedirs(folder, exist_ok=True)
        shutil.copy2(os.path.join(CLIENT_CONFIG, "client.config"), os.path.join(folder, "client.config"))

        for sql in [
            "ivm_system_tables.sql",
            "ivm_compiled_queries_daily_runs_city.sql",
            "ivm_index_daily_runs_city.sql",
            "ivm_upsert_queries_daily_runs_city.sql",
        ]:
            src_path = os.path.join(SOURCE_SQLITE_SCRIPTS, sql)
            dst_path = os.path.join(folder, sql)
            try:
                shutil.copy2(src_path, dst_path)
            except Exception as e:
                print(f"Error copying {src_path} to {dst_path}: {str(e)}")
                traceback.print_exc()

        db_path = os.path.join(folder, "runs.db")
        client_info_path = os.path.join(folder, "client_info.csv")
        csv_path = os.path.join(folder, "test_data.csv")

        if os.path.exists(csv_path):
            os.remove(csv_path)

        nickname, city, run_count, initialized = generate_client_info(client_info_path)
        date = format_date(run_count)
        client_id = int(nickname.split("_")[1])

        if not initialized:
            for sql in [
                "ivm_system_tables.sql",
                "ivm_compiled_queries_daily_runs_city.sql",
                "ivm_index_daily_runs_city.sql",
            ]:
                sql_path = os.path.join(folder, sql)
                with sqlite3.connect(db_path) as conn:
                    execute_sql_file(conn, db_path, sql_path)

        generate_csv(csv_path, nickname, city, date)

        with sqlite3.connect(db_path, isolation_level='IMMEDIATE') as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")

            if not initialized:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS runs (
                        nickname TEXT, 
                        city TEXT, 
                        date TEXT, 
                        start_time TEXT, 
                        end_time TEXT, 
                        steps INTEGER, 
                        heartbeat INTEGER
                    );
                """
                )

            with open(csv_path, 'r') as f:
                reader = csv.reader(f)
                conn.executemany("INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)", list(reader))

            upsert_sql_path = os.path.join(folder, "ivm_upsert_queries_daily_runs_city.sql")
            execute_sql_file(conn, db_path, upsert_sql_path)

        save_client_info(client_info_path, nickname, city, run_count + 1, True)
        update_timestamp(client_id, True, i)

    except Exception as e:
        print(f"Error setting up client folder {folder}: {str(e)}")
        traceback.print_exc()
        raise


def update_timestamp(client_id, initialize, i):
    try:
        folder = os.path.join(TMP_DIR, f"client_{i}")

        # Parse config
        config = parse_client_config(folder)
        server_addr = config.get('server_addr')
        server_port = int(config.get('server_port'))

        if not server_addr or not server_port:
            print(f"Missing server_addr or server_port in client.config for client {client_id}")
            return

        # Create socket connection
        with socket.create_connection((server_addr, server_port), timeout=10) as sock:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            timestamp_bytes = now.encode('utf-8')
            timestamp_size = len(timestamp_bytes)
            print(f"Timestamp size: {timestamp_size}")
            print(f"Client {client_id} timestamp: {now}")

            # Prepare messages
            if initialize:
                message_type = struct.pack('i', 1)
            else:
                message_type = struct.pack('i', 7)

            packed_id = struct.pack('Q', client_id)  # client ID
            packed_size = struct.pack('Q', timestamp_size)  # timestamp size
            close_message = struct.pack('i', 0)  # close message

            # Send all in order
            sock.sendall(message_type)
            sock.sendall(packed_id)
            sock.sendall(packed_size)
            sock.sendall(timestamp_bytes)
            sock.sendall(close_message)

            print(f"Sent timestamp update for client {client_id}: {now}")

    except Exception as e:
        print(f"Error sending timestamp update for client {client_id}: {str(e)}")
        traceback.print_exc()


def compute_window():
    delta = datetime.now() - REFERENCE_TIME
    return int(delta.total_seconds() // (WINDOW_DURATION_HOURS * 3600))


def create_postgres_table_if_not_exists():
    try:
        with psycopg2.connect(SOURCE_POSTGRES_DSN) as pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS rdda_centralized_view_daily_runs_city (
                        nickname VARCHAR,
                        city VARCHAR,
                        date DATE,
                        total_steps BIGINT,
                        generation TIMESTAMPTZ,
                        arrival TIMESTAMPTZ,
                        rdda_window INT,
                        client_id BIGINT,
                        action SMALLINT
                    );
                """
                )
            pg_conn.commit()
    except Exception as e:
        print("Error creating PostgreSQL table:", str(e))
        traceback.print_exc()


def send_to_postgres(i):
    try:
        folder = os.path.join(TMP_DIR, f"client_{i}")
        db_path = os.path.join(folder, "runs.db")

        with sqlite3.connect(db_path) as sqlite_conn:
            rows = sqlite_conn.execute("SELECT * FROM runs").fetchall()

        if not rows:
            return

        now = datetime.now()
        window = compute_window()
        client_id = 0

        enriched = []
        for row in rows:
            nickname, city, date, start, end, steps, _ = row
            client_id = int(nickname.split("_")[1])
            enriched.append(
                (nickname, city, date, steps, now, now, window, client_id, 1)  # generation  # arrival  # action
            )

        with psycopg2.connect(SOURCE_POSTGRES_DSN) as pg_conn:
            with pg_conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO rdda_centralized_view_daily_runs_city (
                        nickname, city, date, total_steps, generation, arrival, rdda_window, client_id, action
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    enriched,
                )
            pg_conn.commit()

        update_timestamp(client_id, False, i)

        with sqlite3.connect(db_path) as sqlite_conn:
            try:
                sqlite_conn.execute("DELETE FROM runs")
            except sqlite3.Error as e:
                print(f"Error deleting rows from SQLite database {db_path}: {str(e)}")
                traceback.print_exc()
                raise

    except Exception as e:
        print(f"Error sending data to Postgres for client {client_id}: {str(e)}")
        traceback.print_exc()

# Add this function to the script

def flush():
    try:
        folder = os.path.join(TMP_DIR, f"client_0")

        # Parse config
        config = parse_client_config(folder)
        server_addr = config.get('server_addr')
        server_port = int(config.get('server_port'))

        if not server_addr or not server_port:
            print(f"Missing server_addr or server_port in client.config for client")
            return

        # Create socket connection
        with socket.create_connection((server_addr, server_port), timeout=10) as sock:

            postgres = "postgres"
            postgres_len = len(postgres)
            view = "daily_runs_city"
            view_len = len(view)

            message_type = struct.pack('i', 8)
            packed_postgres_len = struct.pack('Q', postgres_len)
            packed_postgres = postgres.encode('utf-8')
            packed_view_len = struct.pack('Q', view_len)
            packed_view = view.encode('utf-8')
            packed_close = struct.pack('i', 0)  # close message

            # Send all in order
            sock.sendall(message_type)
            sock.sendall(packed_view_len)
            sock.sendall(packed_view)
            sock.sendall(packed_postgres_len)
            sock.sendall(packed_postgres)
            sock.sendall(packed_close)

            print(f"Flushed")

    except Exception as e:
        print(f"Error flushing")
        traceback.print_exc()


def run_client(client_id):
    try:
        send_to_postgres(client_id)
    except Exception as e:
        print(f"Error running client {client_id}: {str(e)}")
        traceback.print_exc()


def run_cycle(n_clients):
    print(f"--- Setting up {n_clients} client(s) ---")
    for i in range(n_clients):
        try:
            setup_client_folder(i)
        except Exception as e:
            print(f"Failed to setup client {i}: {str(e)}")

    print("--- Generating and sending data ---")
    with ThreadPoolExecutor() as executor:
        executor.map(run_client, range(n_clients))

    print("--- Flushing data ---")
    flush()

    print("\u2714\ufe0f  Cycle complete.\n")


def main():
    parser = argparse.ArgumentParser(description="Setup SQLite clients and push to Postgres periodically.")
    parser.add_argument("N", type=int, help="Number of clients")
    parser.add_argument("H", type=int, help="Interval in hours between runs")
    args = parser.parse_args()

    create_postgres_table_if_not_exists()

    while True:
        try:
            run_cycle(args.N)
            print(f"Sleeping for {args.H} hour(s)...\n")
            time.sleep(args.H * 3600)
        except KeyboardInterrupt:
            print("\nShutting down...")
            break
        except Exception as e:
            print(f"Unexpected error in main loop: {str(e)}")
            traceback.print_exc()
            print("Restarting cycle...")
            time.sleep(60)


if __name__ == "__main__":
    main()
