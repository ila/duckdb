#define DUCKDB_EXTENSION_MAIN

#include "server_extension.hpp"
#include "../compiler/include/compiler_extension.hpp"

#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/planner/planner.hpp"
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <regex>
#include <string.h>
#include <sys/stat.h>
#include <common.hpp>
#include <flush_function.hpp>
#include <run_server.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <rdda_parser.hpp>
#include <signal.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/function/table/table_scan.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/planner/operator/logical_get.hpp>

namespace duckdb {

static void InitializeServer(Connection &con, string &config_path, unordered_map<string, string> &config) {

	con.BeginTransaction();
	CreateSystemTables(config_path, con);

	for (auto &config_item : config) {
		string string_insert =
		    "insert into rdda_settings values ('" + config_item.first + "', '" + config_item.second + "');";
		auto r = con.Query(string_insert);
		if (r->HasError()) {
			throw ParserException("Error while inserting settings: " + r->GetError());
		}
	}
	con.Commit();
}

void ParseJSON(Connection &con, std::unordered_map<string, string> &config, int32_t connfd, hugeint_t client) {

	int32_t size_json;
	auto s = read(connfd, &size_json, sizeof(int32_t));

	std::vector<char> buffer(size_json);

	auto r = read(connfd, buffer.data(), size_json);
	std::cout << "Read " << r << " bytes\n" << std::flush;

	// remove newlines
	std::remove(buffer.begin(), buffer.end(), '\n');

	// todo add schema name
	Appender appender(con, "statistics");
	appender.BeginRow();
	appender.Append<string_t>(string(buffer.begin(), buffer.end()));
	appender.Append<hugeint_t>(client);
	appender.Append<timestamp_t>(Timestamp::GetCurrentTimestamp());
	appender.EndRow();
}

static void LoadInternal(ExtensionLoader &loader) {
	// todo:
	// send statistics to the server

	auto &instance = loader.GetDatabaseInstance();

	// todo fix the hardcoded path
	// todo maybe add schema here?
	string config_path = "../extension/server/";
	string config_file = "server.config";

	// reading config args
	auto config = ParseConfig(config_path, config_file);

	DuckDB db(instance);
	Connection con(db);

	auto client_info = con.TableInfo("rdda_clients");
	auto db_name = con.Query("select current_database();");
	if (db_name->HasError()) {
		throw ParserException("Error while getting database name: ", db_name->GetError());
	}
	auto db_name_str = db_name->GetValue(0, 0).ToString();
	if (!client_info && db_name_str == "rdda_parser") {
		// table does not exist --> the database should be initialized
		Printer::Print("Initializing server!");
		InitializeServer(con, config_path, config);
	}

	// add a parser extension
	auto &db_config = DBConfig::GetConfig(instance);
	auto rdda_parser = RDDAParserExtension();
	db_config.parser_extensions.push_back(rdda_parser);

	// the first argument is the name of the view to flush
	// the second is the database - duckdb, postgres, etc.
	auto flush = PragmaFunction::PragmaCall("flush", FlushFunction, {LogicalType::VARCHAR}, {LogicalType::VARCHAR});
	loader.RegisterFunction(flush);

	auto run_server = PragmaFunction::PragmaCall("run_server", RunServer, {});
	loader.RegisterFunction(run_server);
}

void ServerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
string ServerExtension::Name() {
	return "server";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API const char *server_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
