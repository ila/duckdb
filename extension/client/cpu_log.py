import traceback
import psutil
import time
import json
import psycopg2
from collections import defaultdict
from datetime import datetime
import test_runs_sqlite_parameters as params

OUTPUT_FILE = "cpu_pg_usage_log.csv"
SAMPLE_INTERVAL = 1  # seconds


def get_postgres_table_size(table):
    try:
        with psycopg2.connect(params.SOURCE_POSTGRES_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_total_relation_size(%s);", (table,))
                size = cur.fetchone()[0]
                return size
    except Exception as e:
        print(f"Error querying PostgreSQL: {e}")
        return -1

def monitor(run_number, window_seconds, pg_table):
    print(f"\n[{datetime.now()}] Run {run_number} started...")

    cpu_time_used = 0.0

    prev = psutil.cpu_times()
    start_time = time.time()

    while time.time() - start_time < window_seconds:
        time.sleep(SAMPLE_INTERVAL)
        current = psutil.cpu_times()
        delta = sum([getattr(current, f) - getattr(prev, f) for f in current._fields])
        cpu_time_used += delta
        prev = current

    total_capacity = window_seconds * psutil.cpu_count()
    cpu_percent = (cpu_time_used / total_capacity) * 100

    print(f"[{datetime.now()}] Run {run_number} finished: CPU = {cpu_percent:.2f}%")

    pg_size = get_postgres_table_size(pg_table)
    if pg_size != -1:
        print(f"[{datetime.now()}] PostgreSQL table size: {pg_size} bytes")
    else:
        print(f"[{datetime.now()}] Failed to retrieve PostgreSQL size.")

    mode = 'w' if run_number == 0 else 'a'
    with open(OUTPUT_FILE, mode) as f:
        if run_number == 0:
            f.write("run,total_cpu_usage,storage_size_bytes\n")
        f.write(f"{run_number},{cpu_percent:.2f},{pg_size}\n")



if __name__ == "__main__":
    run = 0
    refresh = params.REFRESH
    runs = params.MAX_RUNS

    flush_interval_minutes = params.FLUSH_INTERVAL
    chunk_interval = (
        flush_interval_minutes / params.NUM_CHUNKS
        if refresh and not params.CENTRALIZED
        else flush_interval_minutes
    )

    if refresh and not params.CENTRALIZED:
        runs *= params.NUM_CHUNKS

    try:
        while run < runs:
            print(f"\n--- Starting chunk ---")
            print(f"Sleeping for {chunk_interval} minutes...")

            monitor(run, chunk_interval * 60, params.FLUSH_NAME)

            run += 1
            print(f"✔️  Cycle {run} complete.\n")

    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Unexpected error in main loop: {str(e)}")
        traceback.print_exc()
        print("Restarting cycle...")
        time.sleep(60)
