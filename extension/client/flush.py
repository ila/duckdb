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


import test_runs_sqlite_parameters as params

# note: this requires postgres installed, role and database created ("ubuntu" in this case)
# setting postgresql.conf with 100 max clients and listening on all addresses
# also pg_hba.conf with "host    all             all             0.0.0.0/0               md5"
# the database is rdda_client

def parse_client_config(folder_path):
    config_path = "/home/ila/Code/duckdb/extension/client/client.config"
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

def flush(flush_name, centralized):

    if centralized:
        folder = os.path.join(params.TMP_DIR, f"client_c_0")
    else:
        folder = os.path.join(params.TMP_DIR, f"client_d_0")

    try:
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
            view = flush_name
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

            print(f"--- Flushed {flush_name} ---")

    except Exception as e:
        print(f"Error flushing")
        traceback.print_exc()


def update_window(update_window_name, centralized):
    if centralized:
        folder = os.path.join(params.TMP_DIR, f"client_c_0")
    else:
        folder = os.path.join(params.TMP_DIR, f"client_d_0")

    try:
        # Parse config
        config = parse_client_config(folder)
        server_addr = config.get('server_addr')
        server_port = int(config.get('server_port'))

        if not server_addr or not server_port:
            print(f"Missing server_addr or server_port in client.config for client")
            return

        # Create socket connection
        with socket.create_connection((server_addr, server_port), timeout=10) as sock:

            view = update_window_name
            view_len = len(view)

            message_type = struct.pack('i', 9)
            packed_view_len = struct.pack('Q', view_len)
            packed_view = view.encode('utf-8')
            packed_close = struct.pack('i', 0)  # close message

            # Send all in order
            sock.sendall(message_type)
            sock.sendall(packed_view_len)
            sock.sendall(packed_view)
            sock.sendall(packed_close)

            print(f"--- Updated window ---")

    except Exception as e:
        print(f"Error updating window")
        traceback.print_exc()

def main():

    run = 0

    flush_name = "daily_runs_city"
    update_window_name = "rdda_centralized_view_daily_runs_city"
    centralized = False
    # flush_name = "runs"
    # update_window_name = "mv_daily_runs_city"
    # centralized = True

    refresh = params.REFRESH

    flush_interval_minutes = params.FLUSH_INTERVAL  # e.g., 20
    chunk_interval = (
        flush_interval_minutes / params.NUM_CHUNKS
        if refresh and not centralized
        else flush_interval_minutes
    )

    try:
        while run < params.MAX_RUNS:
            print(f"\n--- Starting chunk ---")
            time.sleep(chunk_interval * 60)

            # Execute flush every chunk interval
            flush(flush_name, centralized)

            # Only increment run every full flush interval (i.e., after all chunks)
            if refresh and not centralized:
                # Count how many chunks happened and increment only every full interval
                if (run + 1) * params.NUM_CHUNKS % params.NUM_CHUNKS == 0:
                    run += 1
                    update_window(update_window_name, centralized)
                    print(f"✔️  Cycle {run} complete.\n")
            else:
                run += 1
                update_window(update_window_name, centralized)
                print(f"✔️  Cycle {run} complete.\n")

    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Unexpected error in main loop: {str(e)}")
        traceback.print_exc()
        print("Restarting cycle...")
        time.sleep(60)


if __name__ == "__main__":
    main()
