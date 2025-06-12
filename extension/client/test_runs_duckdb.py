import os
import shutil
import argparse
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

# SOURCE_EXECUTABLE = "/home/ubuntu/duckdb/cmake-build-releaseremote/duckdb"
# SOURCE_CONFIG = "/home/ubuntu/duckdb/extension/client/client.config"
SOURCE_EXECUTABLE = "/home/ila/Code/duckdb/cmake-build-debug/duckdb"
SOURCE_CONFIG = "/home/ila/Code/duckdb/extension/client/client.config"
SOURCE_TABLES = "/home/ila/Code/duckdb/extension/client/tables.sql"
TMP_DIR = "/home/tmp_duckdb/"


def setup_client_folder(client_id):
    os.makedirs(TMP_DIR, exist_ok=True)
    folder_name = os.path.join(TMP_DIR, f"client_{client_id}")
    os.makedirs(folder_name, exist_ok=True)

    dest_executable = os.path.join(folder_name, "duckdb")
    dest_config = os.path.join(folder_name, "client.config")
    dest_tables = os.path.join(folder_name, "tables.sql")

    # Check that the source files exist
    if not os.path.isfile(SOURCE_EXECUTABLE):
        raise FileNotFoundError(f"Source executable not found: {SOURCE_EXECUTABLE}")
    if not os.path.isfile(SOURCE_CONFIG):
        raise FileNotFoundError(f"Source config not found: {SOURCE_CONFIG}")

    # Copy if not already in place
    if not os.path.exists(dest_executable):
        shutil.copy2(SOURCE_EXECUTABLE, dest_executable)

    shutil.copy2(SOURCE_CONFIG, dest_config)
    shutil.copy2(SOURCE_TABLES, dest_tables)

    os.chmod(dest_executable, 0o755)  # Ensure it's executable


def run_client(client_id):

    folder_name = os.path.join(TMP_DIR, f"client_{client_id}")
    db_path = os.path.join(folder_name, "runs.db")

    print(f"Generating and sending data for client {client_id}...", flush=True)
    try:
        result = subprocess.run(
            ["./duckdb", db_path, "-c", "PRAGMA test_runs;"],
            cwd=folder_name,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )

    except subprocess.CalledProcessError as e:
        print(f"[{folder_name}] Exception: {e}")
        print(f"[{folder_name}] STDOUT:\n{e.stdout.strip()}")
        print(f"[{folder_name}] STDERR:\n{e.stderr.strip()}")
        raise

    except Exception as e:
        print(f"[{folder_name}] General Exception: {e}")
        raise


def run_cycle(n_clients):

    print(f"--- Setting up {n_clients} client(s) ---")
    # with ThreadPoolExecutor() as executor:
    #     executor.map(setup_client_folder, range(n_clients))
    for i in range(n_clients):
        setup_client_folder(i)

    print("--- Running PRAGMA test ---")
    with ThreadPoolExecutor() as executor:
        executor.map(run_client, range(n_clients))

    print("✔️  Cycle complete.\n")


def main():
    parser = argparse.ArgumentParser(description="Setup DuckDB clients and run PRAGMA test periodically.")
    parser.add_argument("N", type=int, help="Number of clients")
    parser.add_argument("H", type=int, help="Interval in hours between runs")
    args = parser.parse_args()

    while True:
        run_cycle(args.N)
        print(f"Sleeping for {args.H} hour(s)...\n")
        time.sleep(args.H * 3600)


if __name__ == "__main__":
    main()
