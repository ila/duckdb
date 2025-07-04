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


import test_parameters as params

# note: this requires postgres installed, role and database created ("ubuntu" in this case)
# setting postgresql.conf with 100 max clients and listening on all addresses
# also pg_hba.conf with "host    all             all             0.0.0.0/0               md5"
# the database is sidra_client

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

    refresh = params.REFRESH
    runs = params.MAX_RUNS
    centralized = params.CENTRALIZED

    flush_interval_minutes = params.FLUSH_INTERVAL  # e.g., 20
    chunk_interval = (
        flush_interval_minutes / params.NUM_CHUNKS
        if refresh and not centralized
        else flush_interval_minutes
    )
    if refresh and not centralized:
        runs = runs * params.NUM_CHUNKS

    try:
        while run < runs:
            print(f"\n--- Starting chunk ---")
            print(f"Sleeping for {chunk_interval} minutes...")
            time.sleep(chunk_interval * 60)

            flush(params.FLUSH_NAME, centralized)

            # Case 1: update window every chunk (rare)
            if params.UPDATE_WINDOW_EVERY_REFRESH:
                update_window(params.UPDATE_WINDOW_NAME, centralized)

            # Case 2: update only at the end of each full refresh interval (for chunked non-centralized mode)
            if refresh and not centralized and not params.UPDATE_WINDOW_EVERY_REFRESH:
                if (run + 1) % params.NUM_CHUNKS == 0:
                    update_window(params.UPDATE_WINDOW_NAME, centralized)
                    run += 1
                    print(f"✔️  Cycle {run / params.NUM_CHUNKS - 1} complete.\n")
                else:
                    run += 1  # not end of flush window yet
            else:
                # Case 3: centralized or non-refresh (always one flush per run)
                run += 1
                if not refresh:
                    update_window(params.UPDATE_WINDOW_NAME, centralized)
                print(f"✔️  Cycle {run - 1} complete.\n")

    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Unexpected error in main loop: {str(e)}")
        traceback.print_exc()
        print("Restarting cycle...")
        time.sleep(60)


if __name__ == "__main__":
    main()
