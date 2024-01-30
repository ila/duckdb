#include "../compiler/include/compiler_extension.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "include/openivm_benchmark.hpp"

#include <cmath>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <regex>

namespace duckdb {

void ReplaceTableName(string &catalog, string &schema, string& query) {
	// regex pattern to match table names not followed by an alias
	// todo test with alias
	std::regex pattern(R"(\b((?:from|join)\s+)(\w+)(?![^(]*\))\b)", std::regex_constants::icase);
	query = std::regex_replace(query, pattern, "$1" + catalog + "." + schema + ".$2");
}

void RunIVMCrossSystemDemo(string& catalog, string& schema, string& path) {

	// usage: call ivm_demo_postgres('p', 'public', 'file_path');

	// note: I manually changed the engine_version here to make the extension work

	// path is the file with the materialized view definition
	// this assumes (new) data already present in PostgreSQL

	auto query = CompilerExtension::ReadFile(path);
	query = CompilerExtension::SQLToLowercase(query);
	ReplaceTableName(catalog, schema, query); // this is for the input query
	// the materialized view is stored on DuckDB with data imported from PostgreSQL
	auto table = CompilerExtension::ExtractViewName(query); // the table is on PostgreSQL

	const char *user = std::getenv("USER");
	// todo add dbname flag
	string conn_info = "dbname=" + string(user) + " user=" + string(user) + " dbname=dvdrental";

	DuckDB db("/home/ila/Code/duckdb/postgres.db");
	Connection con(db);

	// setting system settings
	con.Query("load '/home/ila/postgres_scanner.duckdb_extension'");
	con.Query("set ivm_files_path='/home/ila/Code/duckdb'"); // todo this does not work
	con.Query("set ivm_catalog_name='" + catalog + "'");
	con.Query("set ivm_schema_name='" + schema + "'");

	auto res =  con.Query("ATTACH '" + conn_info + "' AS p (TYPE postgres);");
	if (res->HasError()) {
		throw Exception("Could not attach to PostgreSQL: " + res->GetError());
	}

	res = con.Query(query);
	if (res->HasError()) {
		throw Exception("Could not create materialized view: " + res->GetError());
	}

	std::cout << "Input query: " << query << "\n";

	auto count = con.Query("SELECT COUNT(*) FROM " + table + ";")->GetValue(0, 0).ToString();
	std::cout << "Rows inserted in the materialized view: " << Format(count) << "\n";
	auto rows = con.Query("SELECT * FROM " + table + ";");
	rows->Print();

	// now triggering the IVM
	// PRAGMA ivm_upsert('catalog', 'schema', 'result')
	string input;
	std::cout << "Enter 'OK' when changes are ready to be applied: ";
	std::cin >> input;

	// check if the input is equal to "OK"
	if (input == "OK") {
		res = con.Query("PRAGMA ivm_upsert('postgres', 'main', '" + table + "');");
		if (res->HasError()) {
			throw Exception("Could not complete IVM: " + res->GetError());
		} else {
			count = con.Query("SELECT COUNT(*) FROM " + table + ";")->GetValue(0, 0).ToString();
			std::cout << "Rows in the materialized view after the update: " << Format(count) << "\n";
		}
	} else {
		std::cout << "Invalid input. Expected 'OK'.\n";
	}

}

} // namespace duckdb