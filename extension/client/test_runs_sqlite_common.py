import os
import shutil
import random
import csv
import sqlite3
import psycopg2
import traceback
import socket
import struct
import json
from datetime import datetime, timedelta
from psycopg2 import DatabaseError, OperationalError
import test_runs_sqlite_parameters as params

# Common utility functions
def chunk_clients(client_list, size):
    """Split the client list into chunks of given size."""
    for i in range(0, len(client_list), size):
        yield client_list[i:i + size]

def get_random_city():
    return random.choice(params.CITIES)

MAIN_CITY = "New York"
RARE_CITIES = [c for c in params.CITIES if c != MAIN_CITY]

def get_random_city_skewed():
    # 90% of clients go to the main city
    if random.random() < 0.9:
        return MAIN_CITY
    # 10% distributed across the remaining 99 cities
    return random.choice(RARE_CITIES)

def format_date(offset_days):
    return (datetime.now() + timedelta(days=offset_days)).strftime('%Y-%m-%d')

def format_time():
    return f"{random.randint(5, 8):02}:{random.randint(0, 59):02}:{random.randint(0, 59):02}"

# Configuration and file operations
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
    city = get_random_city_skewed()
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
            os.remove(path)
        with open(path, 'a') as f:
            writer = csv.writer(f)
            #for _ in range(random.randint(1, 5)):
            for _ in range(1):
                writer.writerow([
                    nickname, city, date, format_time(),
                    format_time(), random.randint(500, 10500),
                    random.randint(60, 140)
                ])
    except Exception as e:
        print(f"Error generating CSV at {path}: {str(e)}")
        traceback.print_exc()

# Database operations
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

# Network operations
def update_timestamp(client_id, initialize, i, client_prefix):
    try:
        folder = os.path.join(params.TMP_DIR, f"{client_prefix}_{i}")
        config = parse_client_config(folder)
        server_addr = config.get('server_addr')
        server_port = int(config.get('server_port'))

        if not server_addr or not server_port:
            print(f"Missing server_addr or server_port in client.config for client {client_id}")
            return

        with socket.create_connection((server_addr, server_port), timeout=10) as sock:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            timestamp_bytes = now.encode('utf-8')
            timestamp_size = len(timestamp_bytes)

            message_type = struct.pack('i', 1 if initialize else 7)
            packed_id = struct.pack('Q', client_id)
            packed_size = struct.pack('Q', timestamp_size)
            close_message = struct.pack('i', 0)

            sock.sendall(message_type)
            sock.sendall(packed_id)
            sock.sendall(packed_size)
            sock.sendall(timestamp_bytes)
            sock.sendall(close_message)
    except Exception as e:
        print(f"Error sending timestamp update for client {client_id}: {str(e)}")
        traceback.print_exc()

# Client lifecycle management
def load_metadata(metadata_dir, metadata_path):
    if not os.path.exists(metadata_dir):
        os.makedirs(metadata_dir, exist_ok=True)
    if not os.path.exists(metadata_path):
        return {"dead_clients": [], "late_clients": {}, "next_client_id": 0}
    with open(metadata_path, "r") as f:
        return json.load(f)

def save_metadata(metadata, metadata_path):
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)


