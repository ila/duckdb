#include "include/rdda/generate_python_script.hpp"

#include <fstream>
#include <duckdb/common/local_file_system.hpp>

namespace duckdb {

void GenerateServerRefreshScript(ClientContext &context, const FunctionParameters &parameters) {

	// dependencies: cmake, ninja, python development headers
	// permissions need to be set for the python package to be built in its folder

	// todo - wait 5 minutes if the database is locked

	auto database_name = parameters.values[0].GetValue<string>();
	// check if the database exists
	LocalFileSystem fs;
	if (!fs.FileExists(database_name)) {
		throw ParserException("Database does not exist!");
	}

	string file_name = "refresh_server_" + database_name + ".py";

	std::ofstream file;
	file.open(file_name);
	file << "# this script is machine generated - do not edit!\n\n";
	file << "#!/usr/bin/env python3\n";
	file << "import importlib.util\n";
	file << "import sys\n";
	file << "import time\n";
	file << "import os\n";
	file << "from datetime import datetime, timedelta\n";
	file << "import subprocess\n\n";
	file << "current_dir = os.getcwd()\n";
	file << "if importlib.util.find_spec('duckdb') is None:\n";
	// we cannot install duckdb from pip, since it does not ship the server extension (containing the flush pragma)
	// we build the duckdb package from source and install it
	// file << "\tsubprocess.check_call([sys.executable, '-m', 'pip', 'install', 'duckdb'])\n\n";
	// save current directory
	file << "\ttry:\n";
	file << "\t\tos.chdir('..')\n\n";

	file << "\t\tenv = os.environ.copy()\n";
	file << "\t\tenv[\"BUILD_PYTHON\"] = \"1\"\n";
	file << "\t\tenv[\"GEN\"] = \"ninja\"\n\n";

	file << "\t\tsubprocess.run([\"make\"], env=env, check=True)\n";
	file << "\t\tsubprocess.run([\"python\", \"setup.py\", \"install\"], cwd=\"tools/pythonpkg\", check=True)\n\n";

	file << "\texcept subprocess.CalledProcessError as e:\n";
	file << "\t\tprint(f\"Command failed with exit code {e.returncode}: {e.cmd}\")\n";
	file << "\t\tprint(f\"Error message: {e.output}\")\n";

	file << "\texcept FileNotFoundError as e:\n";
	file << "\t\tprint(f\"File not found: {e.filename}\")\n";

	file << "\texcept Exception as e:\n";
	file << "\t\tprint(f\"An unexpected error occurred: {e}\")\n\n";

	file << "import duckdb\n\n";
	file << "os.chdir(current_dir)\n\n";

	// initializing variables
	file << "views = []\n";
	file << "windows = []\n";
	file << "refreshes = []\n";
	file << "last_refreshes = []\n";
	file << "last_updates = []\n\n";

	// connecting to the metadata database
	file << "con = duckdb.connect(database = \"rdda_parser.db\", read_only = True)\n";
	// potential exceptions: duckdb.duckdb.IOException
	// last refresh is the last refresh (flush) of the materialized view
	// last update is the last metadata window update
	// todo exception handling
	file << "con.execute(\"select rdda_view_constraints.view_name, rdda_view_constraints.rdda_window, rdda_refresh, last_refresh, last_update from rdda_view_constraints left outer join rdda_current_window on rdda_current_window.view_name = concat('rdda_centralized_view_', rdda_view_constraints.view_name);\")\n";

	file << "for view in con.fetchall():\n";
	file << "\tviews.append(view[0])\n";
	file << "\twindows.append(view[1])\n";
	file << "\trefreshes.append(view[2])\n";
	file << "\tlast_refreshes.append(view[3])\n";
	file << "\tlast_updates.append(view[4])\n";
	file << "con.close()\n\n";

	// now we create the refresh loop
	file << "while True:\n";
	file << "\tcurrent_time = datetime.now()\n";
	file << "\tprint(f'[{datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")}] Waiting to refresh metadata...')\n";
	file << "\tfor i in range(len(views)):\n";
	file << "\t\tif last_updates[i] is not None:\n";
	file << "\t\t\tif current_time - last_updates[i] >= timedelta(hours=windows[i]):\n";
	file << "\t\t\t\t# update the window metadata\n";
	file << "\t\t\t\tprint(f'[{datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")}] Updating window for view: {views[i]}')\n";
	file << "\t\t\t\tcon = duckdb.connect(database = \"rdda_parser.db\")\n";
	// todo exception
	file << "\t\t\t\tupdate_window_query = f\"update rdda_current_window set rdda_window = rdda_window + 1, last_update = now() where view_name = 'rdda_centralized_view_{views[i]}';\"\n";
	file << "\t\t\t\tcon.execute(update_window_query)\n";
	file << "\t\t\t\tcon.close()\n";
	file << "\t\t\t\tlast_updates[i] = current_time\n";
	// we need to check for the last update being not null (this is a left join)
	file << "\t\tif current_time - last_refreshes[i] >= timedelta(hours=windows[i]):\n";
	file << "\t\t\t# refresh the materialized view\n";
	file << "\t\t\tcon = duckdb.connect(database = \"" + database_name + "\")\n";
	file << "\t\t\tpragma_query = f\"PRAGMA flush({views[i]});\"\n";
	file << "\t\t\tcon.execute(pragma_query)\n";
	file << "\t\t\tcon.close()\n";
	file << "\t\t\tprint(f'[{datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")}] Refreshing view: {views[i]}')\n";
	file << "\t\t\tcon = duckdb.connect(database = \"rdda_parser.db\")\n";
	file << "\t\t\tupdate_refresh_query = f\"update rdda_view_constraints set last_refresh = now() where view_name = '{views[i]}';\"\n";
	file << "\t\t\tcon.execute(update_refresh_query)\n";
	file << "\t\t\tcon.close()\n";
	file << "\t\t\tprint(f'[{datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")}] Updating metadata for view: {views[i]}')\n";
	file << "\t\t\tlast_refreshes[i] = current_time\n";
	// every 10 minutes, we check if one of the timestamps elapsed

	file << "\ttime.sleep(600)\n";

	file.close();
}

void GenerateClientRefreshScript(ClientContext &context, const FunctionParameters &parameters) {

	// dependencies: cmake, ninja, python development headers
	// permissions need to be set for the python package to be built in its folder

	// todo - wait 5 minutes if the database is locked

	auto database_name = parameters.values[0].GetValue<string>();
	// check if the database exists
	LocalFileSystem fs;
	if (!fs.FileExists(database_name + ".db")) {
		throw ParserException("Database does not exist!");
	}

	string file_name = "refresh_client_" + database_name + ".py";

	std::ofstream file;
	file.open(file_name);
	file << "# this script is machine generated - do not edit!\n\n";
	file << "#!/usr/bin/env python3\n";
	file << "import importlib.util\n";
	file << "import sys\n";
	file << "import time\n";
	file << "import os\n";
	file << "from datetime import datetime, timedelta\n";
	file << "import subprocess\n\n";
	file << "current_dir = os.getcwd()\n";
	file << "if importlib.util.find_spec('duckdb') is None:\n";
	// we cannot install duckdb from pip, since it does not ship the server extension (containing the flush pragma)
	// we build the duckdb package from source and install it
	// file << "\tsubprocess.check_call([sys.executable, '-m', 'pip', 'install', 'duckdb'])\n\n";
	// save current directory
	file << "\ttry:\n";
	file << "\t\tos.chdir('..')\n\n";

	file << "\t\tenv = os.environ.copy()\n";
	file << "\t\tenv[\"BUILD_PYTHON\"] = \"1\"\n";
	file << "\t\tenv[\"GEN\"] = \"ninja\"\n\n";

	file << "\t\tsubprocess.run([\"make\"], env=env, check=True)\n";
	file << "\t\tsubprocess.run([\"python\", \"setup.py\", \"install\"], cwd=\"tools/pythonpkg\", check=True)\n\n";

	file << "\texcept subprocess.CalledProcessError as e:\n";
	file << "\t\tprint(f\"Command failed with exit code {e.returncode}: {e.cmd}\")\n";
	file << "\t\tprint(f\"Error message: {e.output}\")\n";

	file << "\texcept FileNotFoundError as e:\n";
	file << "\t\tprint(f\"File not found: {e.filename}\")\n";

	file << "\texcept Exception as e:\n";
	file << "\t\tprint(f\"An unexpected error occurred: {e}\")\n\n";

	file << "import duckdb\n\n";
	file << "os.chdir(current_dir)\n\n";

	// initializing variables
	file << "views = []\n";
	file << "windows = []\n";
	file << "refreshes = []\n";
	file << "last_refreshes = []\n";
	file << "last_updates = []\n\n";

	// todo
	// todo - also send the refresh scripts to the client




	file.close();
}



}

