import os
import shutil
import argparse
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

SOURCE_EXECUTABLE = "/home/ubuntu/duckdb/cmake-build-debug/duckdb"
SOURCE_CONFIG = "/home/ubuntu/duckdb/extension/client/client.config"


def setup_client(client_id):
    folder_name = f"client_{client_id}"
    os.makedirs(folder_name, exist_ok=True)

    dest_executable = os.path.join(folder_name, "duckdb")
    dest_config = os.path.join(folder_name, "client.config")

    if not os.path.exists(dest_executable):
        shutil.copy2(SOURCE_EXECUTABLE, dest_executable)
        shutil.copy2(SOURCE_CONFIG, dest_config)
        os.chmod(dest_executable, 0o755)  # Ensure it's executable

    try:
        result = subprocess.run(
            ["./duckdb", "runs.db", "-c", "PRAGMA test_runs;"],
            cwd=folder_name,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode == 0:
            print(f"[{folder_name}] Success:\n{result.stdout.strip()}")
        else:
            print(f"[{folder_name}] Error:\n{result.stderr.strip()}")
    except Exception as e:
        print(f"[{folder_name}] Exception: {e}")


def run_cycle(n_clients):
    print(f"--- Running PRAGMA test for {n_clients} client(s) ---")
    with ThreadPoolExecutor() as executor:
        executor.map(setup_client, range(n_clients))  # Spawns N threads
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
