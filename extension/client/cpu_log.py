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

def get_all_postgres_processes():
    """Find all PostgreSQL processes (main + worker processes)"""
    postgres_pids = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            name = proc.info['name'].lower()
            cmdline = proc.info['cmdline'] or []

            # Check for postgres processes
            if ('postgres' in name or
                    any('postgres' in str(arg).lower() for arg in cmdline)):
                postgres_pids.append(proc.info['pid'])
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return postgres_pids

def monitor_postgres_only(run_number, window_seconds, pg_table, verbose=False):
    print(f"\n[{datetime.now()}] Run {run_number} started...")

    postgres_cpu_percentages = []
    system_cpu_percentages = []
    total_bytes_recv = 0
    postgres_found = False
    sample_count = 0

    # Initial network state
    net_start = psutil.net_io_counters()
    start_time = time.time()

    # Check if PostgreSQL is running at start
    initial_postgres_pids = get_all_postgres_processes()
    if not initial_postgres_pids:
        print("WARNING: No PostgreSQL processes found at start. Will monitor system CPU instead.")
    else:
        postgres_found = True
        print(f"Found {len(initial_postgres_pids)} PostgreSQL processes at start")

    samples_without_postgres = 0

    # Print interval (every 30 seconds by default, or every sample if verbose)
    print_interval = 1 if verbose else max(1, int(30 / SAMPLE_INTERVAL))

    # Initialize process objects and establish baseline
    postgres_processes = {}

    while time.time() - start_time < window_seconds:
        # Get all PostgreSQL processes
        postgres_pids = get_all_postgres_processes()

        # Calculate PostgreSQL-only CPU usage
        postgres_cpu_total = 0
        active_postgres_processes = 0

        if postgres_pids:
            current_processes = {}

            for pid in postgres_pids:
                try:
                    if pid in postgres_processes:
                        # Use existing process object
                        proc = postgres_processes[pid]
                    else:
                        # Create new process object and establish baseline
                        proc = psutil.Process(pid)
                        proc.cpu_percent()  # First call to establish baseline
                        time.sleep(0.01)  # Very short wait

                    # Get actual CPU percentage
                    cpu_percent = proc.cpu_percent()
                    postgres_cpu_total += cpu_percent
                    active_postgres_processes += 1
                    current_processes[pid] = proc

                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            # Update process tracking
            postgres_processes = current_processes
            postgres_found = True
        else:
            samples_without_postgres += 1
            if verbose or (samples_without_postgres % (print_interval * 10) == 0):
                print(f"  WARNING: No PostgreSQL processes found (sample {samples_without_postgres})")

        # Get system-wide CPU for comparison
        system_cpu = psutil.cpu_percent(interval=SAMPLE_INTERVAL)

        if not postgres_cpu_total == 0:
            postgres_cpu_percentages.append(postgres_cpu_total)
        system_cpu_percentages.append(system_cpu)

        # Only print progress occasionally
        if verbose or sample_count % print_interval == 0:
            elapsed = time.time() - start_time
            if postgres_pids:
                print(f"  Progress: {elapsed:.0f}s/{window_seconds}s - PostgreSQL={postgres_cpu_total:.1f}% ({active_postgres_processes} processes), System={system_cpu:.1f}%")
            else:
                print(f"  Progress: {elapsed:.0f}s/{window_seconds}s - PostgreSQL=N/A (no processes), System={system_cpu:.1f}%")

        sample_count += 1

        # Network measurement (less frequent to reduce overhead)
        if sample_count % 10 == 0:
            net_current = psutil.net_io_counters()
            total_bytes_recv = net_current.bytes_recv - net_start.bytes_recv

    # Rest of the function remains the same...
    # Final network measurement
    net_current = psutil.net_io_counters()
    total_bytes_recv = net_current.bytes_recv - net_start.bytes_recv

    # Calculate statistics
    avg_postgres_cpu = sum(postgres_cpu_percentages) / len(postgres_cpu_percentages) if postgres_cpu_percentages else 0
    max_postgres_cpu = max(postgres_cpu_percentages) if postgres_cpu_percentages else 0
    avg_system_cpu = sum(system_cpu_percentages) / len(system_cpu_percentages) if system_cpu_percentages else 0
    max_system_cpu = max(system_cpu_percentages) if system_cpu_percentages else 0

    print(f"\n[{datetime.now()}] Run {run_number} finished:")

    if not postgres_found:
        print("  ERROR: PostgreSQL was never found during monitoring!")
        print("  Returning system CPU measurements instead.")
        print(f"  System CPU: avg={avg_system_cpu:.2f}%, peak={max_system_cpu:.2f}%")
        avg_postgres_cpu = avg_system_cpu
        max_postgres_cpu = max_system_cpu
    elif samples_without_postgres > 0:
        print(f"  WARNING: PostgreSQL processes missing for {samples_without_postgres} samples")
        print(f"  PostgreSQL CPU: avg={avg_postgres_cpu:.2f}%, peak={max_postgres_cpu:.2f}%")
        print(f"  System CPU: avg={avg_system_cpu:.2f}%, peak={max_system_cpu:.2f}%")
    else:
        print(f"  PostgreSQL CPU: avg={avg_postgres_cpu:.2f}%, peak={max_postgres_cpu:.2f}%")
        print(f"  System CPU: avg={avg_system_cpu:.2f}%, peak={max_system_cpu:.2f}%")

    print(f"  Network = {total_bytes_recv / (1024 ** 2):.2f} MB received")
    print(f"  Samples collected: {len(postgres_cpu_percentages)}")

    try:
        pg_size = get_postgres_table_size(pg_table)
        if pg_size != -1:
            print(f"  PostgreSQL table size: {pg_size} bytes")
        else:
            print(f"  Could not retrieve PostgreSQL table size")
    except Exception as e:
        print(f"  Error getting table size: {e}")
        pg_size = -1

    # Write results
    mode = 'w' if run_number == 0 else 'a'
    try:
        with open('postgres_cpu_results.csv', mode) as f:
            if run_number == 0:
                f.write("run,avg_postgres_cpu,peak_postgres_cpu,avg_system_cpu,peak_system_cpu,storage_size_bytes,bytes_received,postgres_found\n")
            f.write(f"{run_number},{avg_postgres_cpu:.2f},{max_postgres_cpu:.2f},{avg_system_cpu:.2f},{max_system_cpu:.2f},{pg_size},{total_bytes_recv},{postgres_found}\n")
    except Exception as e:
        print(f"  Error writing results to CSV: {e}")

    return avg_postgres_cpu, max_postgres_cpu

# Simple version - just PostgreSQL CPU with minimal logging
def monitor_simple_postgres_cpu(run_number, window_seconds, pg_table, verbose=False):
    print(f"\n[{datetime.now()}] Run {run_number} started...")

    postgres_cpu_percentages = []
    total_bytes_recv = 0
    sample_count = 0

    net_start = psutil.net_io_counters()
    start_time = time.time()

    # Check if PostgreSQL is running
    initial_postgres_pids = get_all_postgres_processes()
    if not initial_postgres_pids:
        print("ERROR: No PostgreSQL processes found! Cannot monitor PostgreSQL CPU.")
        return 0, 0
    else:
        print(f"Found {len(initial_postgres_pids)} PostgreSQL processes")

    # Print interval (every 30 seconds by default, or every sample if verbose)
    print_interval = 1 if verbose else max(1, int(30 / SAMPLE_INTERVAL))

    while time.time() - start_time < window_seconds:
        try:
            # Get PostgreSQL CPU usage
            postgres_pids = get_all_postgres_processes()
            postgres_cpu_total = 0

            if not postgres_pids:
                if sample_count == 0 or sample_count % print_interval == 0:
                    print("WARNING: PostgreSQL processes disappeared during monitoring")
                postgres_cpu_total = 0
            else:
                for pid in postgres_pids:
                    try:
                        proc = psutil.Process(pid)
                        postgres_cpu_total += proc.cpu_percent()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue

            postgres_cpu_percentages.append(postgres_cpu_total)

            # Only print progress occasionally (every 30 seconds by default)
            if verbose or sample_count % print_interval == 0:
                elapsed = time.time() - start_time
                print(f"  Progress: {elapsed:.0f}s/{window_seconds}s, Current PostgreSQL CPU: {postgres_cpu_total:.1f}%")

            time.sleep(SAMPLE_INTERVAL)
            sample_count += 1

            # Network (less frequent to reduce overhead)
            if sample_count % 10 == 0:  # Update network stats every 10 samples
                net_current = psutil.net_io_counters()
                total_bytes_recv = net_current.bytes_recv - net_start.bytes_recv

        except Exception as e:
            print(f"Error during monitoring: {e}")
            postgres_cpu_percentages.append(0)

    # Final network measurement
    net_current = psutil.net_io_counters()
    total_bytes_recv = net_current.bytes_recv - net_start.bytes_recv

    if not postgres_cpu_percentages:
        print("ERROR: No CPU data collected!")
        return 0, 0

    avg_postgres_cpu = sum(postgres_cpu_percentages) / len(postgres_cpu_percentages)
    max_postgres_cpu = max(postgres_cpu_percentages)

    print(f"[{datetime.now()}] Run {run_number} finished:")
    print(f"  PostgreSQL CPU: avg={avg_postgres_cpu:.2f}%, peak={max_postgres_cpu:.2f}%")
    print(f"  Samples collected: {len(postgres_cpu_percentages)}")

    return avg_postgres_cpu, max_postgres_cpu

def monitor(run_number, window_seconds, pg_table):
    print(f"\n[{datetime.now()}] Run {run_number} started...")

    cpu_percentages = []
    total_bytes_recv = 0

    # Initial network state
    net_start = psutil.net_io_counters()
    start_time = time.time()

    # Sample CPU usage at regular intervals
    while time.time() - start_time < window_seconds:
        # Get CPU percentage (this is what you want!)
        cpu_percent = psutil.cpu_percent(interval=SAMPLE_INTERVAL)
        cpu_percentages.append(cpu_percent)

        # Network measurement (cumulative)
        net_current = psutil.net_io_counters()
        total_bytes_recv = net_current.bytes_recv - net_start.bytes_recv

    # Calculate average and peak CPU usage
    avg_cpu_percent = sum(cpu_percentages) / len(cpu_percentages) if cpu_percentages else 0
    max_cpu_percent = max(cpu_percentages) if cpu_percentages else 0

    print(f"[{datetime.now()}] Run {run_number} finished:")
    print(f"  Average CPU = {avg_cpu_percent:.2f}%")
    print(f"  Peak CPU = {max_cpu_percent:.2f}%")
    print(f"  Network = {total_bytes_recv / (1024 ** 2):.2f} MB received")

    pg_size = get_postgres_table_size(pg_table)
    if pg_size != -1:
        print(f"[{datetime.now()}] PostgreSQL table size: {pg_size} bytes")
    else:
        print(f"[{datetime.now()}] Failed to retrieve PostgreSQL size.")

    mode = 'w' if run_number == 0 else 'a'
    with open(OUTPUT_FILE, mode) as f:
        if run_number == 0:
            f.write("run,avg_cpu_usage,peak_cpu_usage,storage_size_bytes,bytes_received\n")
        f.write(f"{run_number},{avg_cpu_percent:.2f},{max_cpu_percent:.2f},{pg_size},{total_bytes_recv}\n")

    return avg_cpu_percent, max_cpu_percent



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

    table_name = "rdda_centralized_view_" + params.FLUSH_NAME

    try:
        with psycopg2.connect(params.SOURCE_POSTGRES_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table_name};")
                conn.commit()
                print(f"\n[{datetime.now()}] Deleted all rows from {table_name}")
    except Exception as e:
        print(f"\n[{datetime.now()}] Error deleting rows from PostgreSQL: {e}")

    try:
        while run < runs:
            print(f"\n--- Starting chunk ---")
            print(f"Measuring for {chunk_interval} minutes...")

            monitor_postgres_only(run, chunk_interval * 60, table_name)

            # Now delete everything from the table
            try:
                with psycopg2.connect(params.SOURCE_POSTGRES_DSN) as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"DELETE FROM {table_name};")
                        conn.commit()
                        print(f"\n[{datetime.now()}] Deleted all rows from {table_name}")
            except Exception as e:
                print(f"\n[{datetime.now()}] Error deleting rows from PostgreSQL: {e}")

            print(f"✔️  Cycle {run} complete.\n")
            run += 1

    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Unexpected error in main loop: {str(e)}")
        traceback.print_exc()
        print("Restarting cycle...")
        time.sleep(60)
