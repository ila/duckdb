## Client repository and SIDRA benchmarks
This repository contains:
* The client extension able to run SIDRA client-side.
* The SIDRA benchmarks, which are used to test the entire architecture.

### Benchmark files:
* `ansible_scripts/playbook.yml`: Ansible playbook to run the benchmarks.
* `test_centralized.py`: A test script that runs the benchmarks in a centralized architecture.
* `test_decentralized.py`: A test script that runs the benchmarks in the SIDRA decentralized architecture.
* `test_commons.py`: Common functions used by the test scripts.
* `test_parameters.py`: Parameters used by the test scripts.
* `cpu_log.py`: A script to log CPU, network and storage usage during the benchmarks.
* `client.config`: Configuration file for the client nodes.
* `flush.py`: Script to orchestrate the flush of the staging area.
* `plot.R`: R script to plot the results of the benchmarks.

All the other files are either source code or examples not pertinent to the paper.

### Configuration of the architecture
To run the benchmark, one needs:
* A device to orchestrate the clients and run the playbooks (e.g., a laptop); this can be the same as the server but results might be skewed in this case, and the AWS keys must be present on this device.
* A server to run the SIDRA server-side code (we use an AWS EC2 `m7g.xxl` instance; other instances will still work but might yield different results).
* AWS credentials to deploy the playbook.

### Setting up - orchestrator device
The requirements for the orchestrator device are:
* Python 3.8 or higher
* Ansible 2.9 or higher
* `aws-cli`
* 
todo one needs a fork of the repo to propagate parameters?
Download the repo on your laptop:
```bash
git clone TODO
cd TODO
```
Edit the `client.config` file to set the IP address of the server (we assume the server is reachable externally). Do not change anything else.

todo instructions to edit the playbook

Edit the `test_parameters.py` file to set the parameters of the benchmarks (e.g., number of clients, number of iterations, etc.). This file should be edited for each benchmark - configurations for the evaluation in the paper is provided below.
Push the changes to your fork of the repository (??).

### Setting up - server
The requirements for the server are:
* PostgreSQL 15 or higher
* Python 3.8 or higher

Create the user `sidra` and the database `sidra` as superuser with password `test`:
```bash
sudo -u postgres createuser --superuser sidra
sudo -u postgres createdb --owner=sidra sidra
sudo -u postgres psql -c "ALTER USER sidra WITH PASSWORD 'test';"
```
Download and compile this repository:
```bash
git clone todo
cd todo
make todo
cd build/release
```
Run `duckdb` to create the database schema (we assume the database file to be in the same location of the `duckdb` build:
```bash
./duckdb activity_metrics.db > ../../extension/client/sql_scripts/create_tables_server.sql
```
Then run the server (and keep it running either in the foreground or in a screen session):
```bash
./duckdb
pragma run_server;
```




### Benchmark configurations