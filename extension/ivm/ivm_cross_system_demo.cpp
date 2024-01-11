#include "../compiler/include/compiler_extension.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "include/ivm_benchmark.hpp"

#include <cmath>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <libpq-fe.h>
#include <random>
#include <regex>

namespace duckdb {

void ReplaceTableName(string& query) {
	// regex pattern to match table names not followed by an alias
	// todo test with alias
	std::regex pattern(R"(\b((?:from|join)\s+)(\w+)(?![^(]*\))\b)", std::regex_constants::icase);
	query = std::regex_replace(query, pattern, "$1p.public.$2");
}

void RunIVMCrossSystemDemo(string& path) {

	// usage: call ivm_demo_postgres('file_path');
	// we assume the schema 'public' and the attached database 'p'

	// note: I manually changed the engine_version here to make the extension work

	// path is the file with the materialized view definition
	// this assumes (new) data already present in PostgreSQL

	// load '/home/ila/postgres_scanner.duckdb_extension';

	auto query = CompilerExtension::ReadFile(path);
	query = CompilerExtension::SQLToLowercase(query);
	ReplaceTableName(query); // this is for the input query
	// the materialized view is stored on DuckDB with data imported from PostgreSQL
	auto table = CompilerExtension::ExtractViewName(query); // the table is on PostgreSQL

	const char *user = std::getenv("USER");
	string conn_info = "dbname=" + string(user) + " user=" + string(user);

	DuckDB db("/home/ila/Code/duckdb/postgres.db");
	Connection con(db);

	//auto res = con.Query("load '/home/ila/postgres_scanner.duckdb_extension'");
	// ATTACH 'dbname=ila user=ila' as p (type postgres);
	// todo monday: bug here when creating delta tables on postgres
	// implement regex function to extract schema and alias and put it in front of delta tables
	// will the postgres query plan work??
	con.Query("load '/home/ila/postgres_scanner.duckdb_extension'");
	con.Query("set ivm_files_path='/home/ila/Code/duckdb'");
	auto res =  con.Query("ATTACH '" + conn_info + "' AS p (TYPE postgres);");
	if (res->HasError()) {
		throw Exception("Could not attach to PostgreSQL: " + res->GetError());
	}

	res = con.Query(query);
	if (res->HasError()) {
		throw Exception("Could not create materialized view: " + res->GetError());
	}

	// now triggering the IVM
	// PRAGMA ivm_upsert('catalog', 'schema', 'result')
	res = con.Query("PRAGMA ivm_upsert('postgres', 'main', '" + table + "');");
	if (res->HasError()) {
		throw Exception("Could not complete IVM: " + res->GetError());
	}


}

} // namespace duckdb