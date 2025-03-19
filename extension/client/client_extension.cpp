#define DUCKDB_EXTENSION_MAIN

#include "client_extension.hpp"
#include "client_functions.hpp"

#include "../server/include/common.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include <fcntl.h>
#include <iostream>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <duckdb/main/extension_util.hpp>

namespace duckdb {

static void Refresh(ClientContext &context, const FunctionParameters &parameters) {
	string config_path = "../extension/client/";
	auto view_name = StringValue::Get(parameters.values[0]);
	auto con = Connection(*context.db);
	auto timestamp = RefreshMaterializedView(view_name, con);
	// we need to send this timestamp to the server (event time)
	auto sock = SendResults(view_name, timestamp, con, config_path);
	CloseConnection(sock);
}


static void InsertClient(Connection &con, unordered_map<string, string> &config, uint64_t id, string_t timestamp) {

	string table_name;
	if (config["schema_name"] != "main") {
		table_name = config["schema_name"] + ".client_information";
	} else {
		table_name = "client_information";
	}
	string query = "insert or ignore into " + table_name + " values (" + std::to_string(id) + ", '" + timestamp.GetString() + "', NULL);";
	auto r = con.Query(query);
	if (r->HasError()) {
		throw ParserException("Error while inserting client information: " + r->GetError());
	}
}

static int32_t GenerateClientInformation(Connection &con, unordered_map<string, string> &config) {

	uint64_t id;
	string timestamp_string;

	// checking that there is only one client
	string query = "select * from client_information;";
	auto r = con.Query(query);
	if (r->RowCount() > 0) {
		// this can happen for example if a connection fails the first time
		id = r->GetValue(0, 0).GetValue<uint64_t>();
		timestamp_string = r->GetValue(1, 0).ToString();
	} else {
		// id, creation, last_update
		RandomEngine engine;
		id = engine.NextRandomInteger64();
		timestamp_t timestamp = Timestamp::GetCurrentTimestamp();
		timestamp_string = Timestamp::ToString(timestamp);
		InsertClient(con, config, id, timestamp_string);
	}

	int32_t sock = ConnectClient(config);

	client_messages message = new_client;
	auto timestamp_size = timestamp_string.size();

	send(sock, &message, sizeof(int32_t), 0);
	send(sock, &id, sizeof(uint64_t), 0);
	send(sock, &timestamp_size, sizeof(size_t), 0);
	send(sock, timestamp_string.c_str(), timestamp_string.size(), 0);

	// now receive the queries
	size_t size;
	read(sock, &size, sizeof(size_t));
	char *buffer = new char[size];
	read(sock, buffer, size);
	string queries(buffer, size);
	r = con.Query(queries);
	// we cannot wrap this into a commit/rollback block because the materialized view depends on the previous table
	if (r->HasError()) {
		Printer::Print("Error while executing queries: " + r->GetError());
		Printer::Print("Attempting to continue execution...");
	}
	// now update the last_update
	auto update = "update client_information set last_update = '" + timestamp_string + "' where id = " + std::to_string(id) + ";";
	r = con.Query(update);
	if (r->HasError()) {
		throw ParserException("Error while updating client information: " + r->GetError());
	}
	return sock;
}

void InitializeClient(ClientContext &context, const FunctionParameters &parameters) {
	string config_path = "../extension/client/";
	string config_file = "client.config";
	auto config = ParseConfig(config_path, config_file);

	DuckDB db(*context.db);
	Connection con(db);

	CreateSystemTables(config_path, con);
	auto sock = GenerateClientInformation(con, config);
	CloseConnection(sock);

}

static void InsertChunks(const std::unique_ptr<MaterializedQueryResult> &result,
                         const unique_ptr<TableDescription> &view_info,
                         Connection &con) {

	for (auto &chunk : result->Collection().Chunks()) {
		// appending one chunk at the time in order to free memory
		con.Append(*view_info, chunk);
	}
}

static void SendJSON(std::unordered_map<string, string> &config, int32_t sock) {
	// read file in memory and send it over
	int32_t fd = open((config["db_path"] + "profile_output.json").c_str(), O_RDONLY);
	int32_t len = lseek(fd, 0, SEEK_END);
	void *buffer = mmap(0, len, PROT_READ, MAP_SHARED, fd, 0);

	send(sock, &len, sizeof(len), 0);
	send(sock, buffer, len, 0);
}

static void LoadInternal(DatabaseInstance &instance) {
	string config_path = "../extension/client/";
	string config_file = "client.config";

	// todo path is still hardcoded, fix this
	// todo error handling if the path is wrong
	// note: serializer might break (also in server)
	DuckDB db(instance);
	Connection con(db);
	con.Query("PRAGMA enable_profiling=json");
	con.Query("PRAGMA profile_output='profile_output.json'");

	// todo - return "false" if error
	auto initialize_client = PragmaFunction::PragmaCall("initialize_client", InitializeClient, {});
	ExtensionUtil::RegisterFunction(instance, initialize_client);

	auto refresh = PragmaFunction::PragmaCall("refresh", Refresh, {LogicalType::VARCHAR});
	ExtensionUtil::RegisterFunction(instance, refresh);

}

void ClientExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string ClientExtension::Name() {
	return "client";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void client_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *client_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
