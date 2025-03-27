#include "include/compiler_extension.hpp"

#include <fstream>

namespace duckdb {

void GeneratePythonRefreshScript(ClientContext &context, const FunctionParameters &parameters) {
	string path = "";
	string file_name = "test.py";

	std::ofstream file;
	file.open(path + file_name);
	file << "# this script is machine generated, do not edit\n\n";
	file << "#!/usr/bin/env python3\n";
	file << "import importlib.util\n";
	file << "import sys\n";
	file << "import subprocess\n\n";
	file << "if importlib.util.find_spec('duckdb') is None:\n";
	file << "\tsubprocess.check_call([sys.executable, '-m', 'pip', 'install', 'duckdb'])\n\n";
	// todo exception handling
	file << "import duckdb\n\n";

	// connecting to the metadata database
	file << "con = duckdb.connect(database = \"rdda_parser.db\", read_only = True)\n";
	// potential exceptions: duckdb.duckdb.IOException
	file << "con.execute(\"select * from rdda_tables where is_view = true\")\n";
	file << "for view in con.fetchall():\n"; // todo save this somewhere so we close the connection early
	file << "\tprint(view)\n";
	file << "con.close()\n\n";


	file.close();
}



}