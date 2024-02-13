#include "../compiler/include/compiler_extension.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "include/ivm_benchmark.hpp"
#include "../compiler/include/logical_plan_to_string.hpp"

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
	// Regex pattern to match table names not followed by an alias
	std::regex pattern(R"(\b(?:FROM|JOIN|UPDATE|INTO|TABLE)\s+(\w+)\b(?! AS \w+))", std::regex_constants::icase);
	std::regex_replace(query, pattern, "p.public.$1");
}

void RunLogicalPlanToString(string& sql_string){
	Printer::Print("Path is: "+ sql_string);
	DuckDB db("../../data/testdb.db");
	// todo:
	// if (!context.db->config.options.database_path.empty()) {
	// 	db_path = context.db->GetFileSystem().GetWorkingDirectory();
	// }
	Connection con(db);
	con.BeginTransaction();
	Planner plan(*con.context);
	Parser parser;
	parser.ParseQuery(sql_string);
	auto statement = parser.statements[0].get();
	plan.CreatePlan(statement->Copy());

	string planString = LogicalPlanToString(plan.plan);	
	Printer::Print("String: " + planString);
	con.Commit();
}

void RunIVMCrossSystemDemo(string& path) {

	// usage: call ivm_demo_postgres('file_path');
	// we assume the schema 'public' and the attached database 'p'

	// note: I manually changed the engine_version here to make the extension work

	// path is the file with the materialized view definition
	// this assumes (new) data already present in PostgreSQL

	// load '/home/ila/postgres_scanner.duckdb_extension';

	
	// plan.CreatePlan()

	// auto query = CompilerExtension::ReadFile(path);
	// // the materialized view is stored on DuckDB with data imported from PostgreSQL
	// auto table = CompilerExtension::ExtractViewName(query); // the table is on PostgreSQL

	// const char *user = std::getenv("USER");
	// string conn_info = "user=" + string(user);

	// DuckDB db(nullptr);
	// Connection con(db);

	//auto res = con.Query("load '/home/ila/postgres_scanner.duckdb_extension'");
	// ATTACH 'dbname=ila user=ila' as p (type postgres);

	/*
	auto attach_string = "ATTACH 'dbname=" + user + " user="+ user + "' as p (type postgres);";
	auto res =  con.Query("ATTACH '" + conn_info + "' AS postgres_db;");
	if (res->HasError()) {
		throw Exception("Could not attach to PostgreSQL: " + res->GetError());
	}

	res = con.Query("select count(*) from groups");
	res->Print(); */

	/*
	res = con.Query(query);
	if (res->HasError()) {
		throw Exception("Could not create materialized view: " + res->GetError());
	}

	// now triggering the IVM
	// PRAGMA ivm_upsert('catalog', 'schema', 'result')
	res = con.Query("PRAGMA ivm_upsert('memory', 'main', '" + table + "');");
	if (res->HasError()) {
		throw Exception("Could not complete IVM: " + res->GetError());
	}
	 */



}

} // namespace duckdb