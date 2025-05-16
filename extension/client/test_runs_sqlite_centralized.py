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
import threading
from itertools import repeat

from psycopg2 import DatabaseError
from psycopg2 import OperationalError

import test_runs_sqlite_parameters as params
import test_runs_sqlite_common as common

import json

CLIENT_METADATA_DIR = os.path.join(params.TMP_DIR, "client_c_metadata")
CLIENT_METADATA_PATH = os.path.join(CLIENT_METADATA_DIR, "metadata.json")

# note: this requires postgres installed, role and database created ("ubuntu" in this case)
# setting postgresql.conf with 100 max clients and listening on all addresses
# also pg_hba.conf with "host    all             all             0.0.0.0/0               md5"
# the database is rdda_client
# 2gb shared buffers, 1000 connections


def setup_client_folder(i):
    folder = os.path.join(params.TMP_DIR, f"client_c_{i}")
    try:
        os.makedirs(folder, exist_ok=True)
        shutil.copy2(os.path.join(params.CLIENT_CONFIG, "client.config"), os.path.join(folder, "client.config"))

        for sql in [
            "ivm_compiled_queries_runs.sql",
        ]:
            src_path = os.path.join(params.SOURCE_SQLITE_SCRIPTS, sql)
            dst_path = os.path.join(folder, sql)
            try:
                shutil.copy2(src_path, dst_path)
            except Exception as e:
                print(f"Error copying {src_path} to {dst_path}: {str(e)}")
                traceback.print_exc()

        db_path = os.path.join(folder, "runs.db")
        client_info_path = os.path.join(folder, f"client_c_{i}_info.csv")
        csv_path = os.path.join(folder, "test_data.csv")

        if os.path.exists(csv_path):
            os.remove(csv_path)

        nickname, city, run_count, initialized = common.generate_client_info(client_info_path)
        date = common.format_date(run_count)
        client_id = int(nickname.split("_")[1])

        common.generate_csv(csv_path, nickname, city, date)

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

        common.save_client_info(client_info_path, nickname, city, run_count + 1, True)
        # update_timestamp(client_id, True, i)

    except Exception as e:
        print(f"Error setting up client folder {folder}: {str(e)}")
        traceback.print_exc()
        raise

def create_postgres_table_if_not_exists():
    try:
        with psycopg2.connect(params.SOURCE_POSTGRES_DSN) as pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS rdda_centralized_view_runs (
                        nickname       TEXT,
                        city           TEXT,
                        date           TEXT, 
                        start_time     TEXT, 
                        end_time       TEXT,
                        steps          INTEGER,
                        heartbeat_rate INTEGER,
                        generation TIMESTAMPTZ,
                        arrival TIMESTAMPTZ,
                        rdda_window INT,
                        client_id BIGINT,
                        action SMALLINT);
                """
                )
            pg_conn.commit()
    except Exception as e:
        print("Error creating PostgreSQL table:", str(e))
        traceback.print_exc()


def send_to_postgres(i, run):
    client_id = -1
    folder = os.path.join(params.TMP_DIR, f"client_c_{i}")
    db_path = os.path.join(folder, "runs.db")

    try:
        # Step 1: Read from SQLite
        with sqlite3.connect(db_path) as sqlite_conn:
            rows = sqlite_conn.execute("SELECT * FROM runs").fetchall()

        if not rows:
            raise ValueError(f"No rows found in SQLite for client {i}, cannot proceed.")

        # Step 2: Enrich the rows
        now = datetime.now()
        enriched = []
        for row in rows:
            nickname, city, date, start, end, steps, heartbeat = row
            client_id = int(nickname.split("_")[1])
            enriched.append((
                nickname, city, date, start, end, steps, heartbeat,
                now, now, run, client_id, 1  # generation, arrival, rdda_window, client_id, action
            ))

        # Step 3: Send to PostgreSQL
        try:
            with psycopg2.connect(params.SOURCE_POSTGRES_DSN) as pg_conn:
                with pg_conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO rdda_centralized_view_runs (
                            nickname, city, date, start_time, end_time, steps, heartbeat_rate,
                            generation, arrival, rdda_window, client_id, action
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        enriched
                    )
                pg_conn.commit()
        except (OperationalError, DatabaseError) as e:
            print(f"❌ PostgreSQL error for client {i}: {e}")
            traceback.print_exc()
            raise

        # Step 4: Cleanup SQLite
        try:
            with sqlite3.connect(db_path) as sqlite_conn:
                sqlite_conn.execute("DELETE FROM runs")
        except sqlite3.Error as e:
            print(f"❌ Failed to delete rows from SQLite for client {i}: {e}")
            traceback.print_exc()
            raise

    except ValueError as ve:
        print(f"⚠️ {ve}")
        raise  # re-raise if you want to fail the pipeline
    except Exception as e:
        print(f"🔥 Unexpected error for client {i} (client_id: {client_id}): {e}")
        traceback.print_exc()
        raise


def flush():
    try:
        folder = os.path.join(params.TMP_DIR, f"client_c_0")

        # Parse config
        config = common.parse_client_config(folder)
        server_addr = config.get('server_addr')
        server_port = int(config.get('server_port'))

        if not server_addr or not server_port:
            print(f"Missing server_addr or server_port in client.config for client")
            return

        # Create socket connection
        with socket.create_connection((server_addr, server_port), timeout=10) as sock:

            postgres = "postgres"
            postgres_len = len(postgres)
            view = "runs"
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

    except Exception as e:
        print(f"Error flushing")
        traceback.print_exc()


def update_window():
    try:
        folder = os.path.join(params.TMP_DIR, f"client_c_0")

        # Parse config
        config = common.parse_client_config(folder)
        server_addr = config.get('server_addr')
        server_port = int(config.get('server_port'))

        if not server_addr or not server_port:
            print(f"Missing server_addr or server_port in client.config for client")
            return

        # Create socket connection
        with socket.create_connection((server_addr, server_port), timeout=10) as sock:

            view = "mv_daily_runs_city"
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

def run_client(client_id, run):
    try:
        send_to_postgres(client_id, run)
    except Exception as e:
        print(f"Error running client {client_id}: {str(e)}")
        traceback.print_exc()


def run_cycle(initial_clients, run):
    metadata = common.load_metadata(CLIENT_METADATA_DIR, CLIENT_METADATA_PATH)

    # if some clients are late, the cycle will finish before the "0" percentage is applied
    dead = set(metadata.get("dead_clients", []))
    late = metadata.get("late_clients", {})
    next_client_id = metadata.get("next_client_id", 0)

    # if params.LATE_RATE == 0:
    #     late = {}

    all_clients = list(range(next_client_id))
    alive_clients = [cid for cid in all_clients if cid not in dead and cid not in late]

    # Dynamically increase target active clients, capped at MAX_CLIENTS
    target_clients = int(min(params.MAX_CLIENTS, initial_clients * ((1 + params.NEW_RATE) ** run)))

    # Sample deaths
    num_to_die = int(len(alive_clients) * params.DEATH_RATE) if alive_clients else 0
    dying_clients = random.sample(alive_clients, min(num_to_die, len(alive_clients)))

    dead.update(dying_clients)

    # Sample new late clients (exclude already-late ones)
    alive_after_death = [cid for cid in alive_clients if cid not in dying_clients]
    eligible_for_new_late = [cid for cid in alive_after_death if str(cid) not in late]
    num_late = int(len(eligible_for_new_late) * params.LATE_RATE) if eligible_for_new_late else 0
    new_late_clients = random.sample(eligible_for_new_late, min(num_late, len(eligible_for_new_late)))

    for cid in new_late_clients:
        late[str(cid)] = random.randint(1, 5)

    # Process late countdown
    late_active = []
    still_late = {}
    old_late_info = {}
    for cid_str, delay in late.items():
        delay -= 1
        if delay <= 0:
            late_active.append(int(cid_str))
        else:
            still_late[cid_str] = delay
            old_late_info[int(cid_str)] = delay
    late = still_late

    # Determine how many new clients we can add
    current_total_clients = next_client_id
    available_slots = params.MAX_CLIENTS - current_total_clients
    if run == 0:
        num_new = min(target_clients, available_slots)
    else:
        num_new = min(max(1, int(target_clients * params.NEW_RATE)), available_slots)
    new_clients = list(range(next_client_id, next_client_id + num_new))
    next_client_id += num_new

    # Select old clients to meet target client count
    remaining_slots = target_clients - len(new_clients)
    old_clients = alive_after_death.copy()
    random.shuffle(old_clients)
    selected_existing = old_clients[:max(0, remaining_slots - len(late_active))]

    # Final list of active clients (deduplicated to prevent repeats)
    active_clients = sorted(set(selected_existing + late_active + new_clients))

    # 📊 DEBUG INFO
    print("\n=== 🌀 Cycle Summary ===")
    print(f"▶️  Run number: {run}")
    print(f"👥 Active clients ({len(active_clients)}): {active_clients}")
    print(f"🆕 New clients ({len(new_clients)}): {new_clients}")
    print(f"🐌 New late clients ({len(new_late_clients)}): {new_late_clients}")
    print(f"🕰️ Late clients still pending ({len(old_late_info)}): {old_late_info}")
    print(f"💀 Dead clients this run ({len(dying_clients)}): {dying_clients}")
    print("========================\n")

    # Generate and send data
    print("--- Initializing client folders ---")
    for i in active_clients:
        try:
            setup_client_folder(i)
        except Exception as e:
            print(f"Error setting up client folder {i}: {str(e)}")
            traceback.print_exc()

    print("--- Generating and sending data in chunks ---")
    for i, chunk in enumerate(common.chunk_clients(active_clients, params.CHUNK_SIZE)):
        print(f"🧩 Dispatching chunk {i + 1}/{(len(active_clients) // params.CHUNK_SIZE) + 1}")
        with ThreadPoolExecutor(max_workers=params.MAX_CONCURRENT_CLIENTS) as executor:
            executor.map(run_client, chunk, repeat(run))
        if i < len(active_clients) // params.CHUNK_SIZE:
            time.sleep(params.CLIENT_DISPATCH_INTERVAL)  # e.g., 5 seconds


# Save metadata
    metadata["dead_clients"] = list(dead)
    metadata["late_clients"] = late
    metadata["next_client_id"] = next_client_id
    common.save_metadata(metadata, CLIENT_METADATA_DIR + CLIENT_METADATA_PATH)


def main():
    if not os.path.exists(params.TMP_DIR):
        os.makedirs(params.TMP_DIR, exist_ok=True)

    create_postgres_table_if_not_exists()
    run = 0

    while run < params.MAX_RUNS:
        try:
            start_time = time.time()
            print(f"\n--- Starting cycle {run} ---")

            run_cycle(params.INITIAL_CLIENTS, run)
            run += 1

            print("✔️  Cycle complete.\n")

            # Calculate remaining time to sleep
            elapsed = time.time() - start_time
            flush_interval = params.FLUSH_INTERVAL * 60 + params.SLEEP_INTERVAL
            remaining = flush_interval - elapsed

            total_sleep = max(0, remaining)
            if run < params.MAX_RUNS:
                print(f"Sleeping for {int(total_sleep)} seconds (to keep cycle ~{flush_interval // 60} min)...\n")
                time.sleep(total_sleep)

        except KeyboardInterrupt:
            print("\n🚨 Interrupted by user. Exiting gracefully.")

        finally:
            run += 1


if __name__ == "__main__":
    main()
