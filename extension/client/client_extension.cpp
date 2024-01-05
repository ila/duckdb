#define DUCKDB_EXTENSION_MAIN

#include "client_extension.hpp"

#include "../server/include/common.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/planner.hpp"

#include <fcntl.h>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace duckdb {

static int32_t ConnectClient(unordered_map<string, string> &config) {

	int sock = 0, client_fd;
	struct sockaddr_in serv_addr;

	if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		printf("\n Socket creation error \n");
	}

	struct hostent *h;

	if ((h = gethostbyname(config["server_addr"].c_str())) == nullptr) { // lookup the hostname
		printf("Unknown host\n");
	}

	memset(&serv_addr, '\0', sizeof(serv_addr));                                // zero structure out
	serv_addr.sin_family = AF_INET;                                             // match the socket() call
	memcpy((char *)&serv_addr.sin_addr.s_addr, h->h_addr_list[0], h->h_length); // copy the address
	serv_addr.sin_port = htons(stoi(config["server_port"]));

	if ((client_fd = connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr))) < 0) {
		printf("\nConnection failed %i\n", client_fd);
		std::cout << errno << "\n";
	}

	// std::cout << "Connected, status " << client_fd << "\n";
	return sock;
}

static void InsertClient(Connection &con, unordered_map<string, string> &config, uint64_t id, string &timestamp) {

	string table_name;
	if (config["schema_name"] != "main") {
		table_name = config["schema_name"] + ".client_information";
	} else {
		table_name = "client_information";
	}
	Appender appender(con, table_name);
	// todo bug here
	appender.AppendRow(id, timestamp, timestamp, timestamp);
}

static void GenerateClientInformation(Connection &con, unordered_map<string, string> &config) {

	// id, creation, last_update, last_result
	auto id = UUID::GenerateRandomUUID();
	auto timestamp = Timestamp::GetCurrentTimestamp();
	auto timestamp_string = Timestamp::ToString(timestamp);

	InsertClient(con, config, id.lower, timestamp_string);

	// todo exception handling
	int32_t sock = ConnectClient(config);

	client_messages message = new_client;

	send(sock, &message, sizeof(int32_t), 0);
	send(sock, &id.lower, sizeof(uint64_t), 0);
	send(sock, &timestamp_string, timestamp_string.size(), 0);
}

static void InitializeClient(string &config_path, unordered_map<string, string> &config) {
	DuckDB db(config["db_path"] + config["db_name"]);
	Connection con(db);

	CreateSystemTables(config_path, config["db_path"], config["db_name"], config["schema_name"], con);
	GenerateClientInformation(con, config);
}

static void SendChunks(std::unique_ptr<MaterializedQueryResult> &result, int32_t sock) {

	auto &collection = result->Collection();
	idx_t num_chunks = collection.ChunkCount();
	std::cout << "Chunks: " << num_chunks << "\n";
	send(sock, &num_chunks, sizeof(idx_t), 0);

	for (auto &chunk : collection.Chunks()) {
		std::cout << "Sending chunk\n";
		MemoryStream target;
		BinarySerializer serializer(target);
		serializer.Begin();
		chunk.Serialize(serializer);
		serializer.End();

		auto data = target.GetData();
		idx_t len = target.GetPosition();

		send(sock, &len, sizeof(ssize_t), 0);
		std::cout << "Sent chunk len " << len << "\n";
		send(sock, data, len, 0);
		std::cout << "Sent chunk\n";
	}
}

static void InsertChunks(std::unique_ptr<MaterializedQueryResult> &result, unique_ptr<TableDescription> view_info,
                         Connection &con) {

	auto &collection = result->Collection();
	idx_t num_chunks = collection.ChunkCount();
	std::cout << "Chunks: " << num_chunks << "\n";

	for (auto &chunk : collection.Chunks()) {
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
	std::cout << "Sent JSON\n";
}

static void LoadInternal(DatabaseInstance &instance) {

	string config_path = "/home/ila/Code/duckdb/extension/client/";
	string config_file = "client.config";

	// reading config args
	// todo path is still hardcoded, fix this
	auto config = ParseConfig(config_path, config_file);

	// todo error handling if the path is wrong
	// note: this hangs with the new duckdb version
	// note: also serializer might break (also in server)
	DuckDB db(config["db_name"]);
	Connection con(db); // this *should* create the database if it does not exist

	int32_t sock = ConnectClient(config);
	client_messages message;

	// search if we should initialize the client (first execution)
	auto table_info = con.TableInfo(config["schema_name"], "client_information");
	if (!table_info) {
		// table does not exist --> the database should be initialized
		message = new_client;
		send(sock, &message, sizeof(int32_t), 0);
		InitializeClient(config_path, config);
	}

	// client connected, ready to do operations
	uint8_t query_execution_time_hours = std::stoi(config["query_execution_time_hours"]);

	con.Query("PRAGMA enable_profiling=json");
	con.Query("PRAGMA profile_output='profile_output.json'");

	// extract the queries we need to execute
	auto queries = con.Query("select view_name, query from rdda_queries;");

	while (true) {

		for (idx_t row_idx = 0; row_idx < queries->RowCount(); row_idx++) {
			string query_name = queries->GetValue(0, row_idx).ToString();
			string query = queries->GetValue(1, row_idx).ToString();

			// now execute and send
			// should we close the socket at each execution?

			// inserting results in a decentralized view
			string decentralized_view_name = "rdda_decentralized_view_" + query_name;
			auto result = con.Query(query);
			auto view_info = con.TableInfo(config["schema_name"], decentralized_view_name);
			if (view_info) {
				// the view exists, just append
				InsertChunks(result, move(view_info), con);
			} else {
				// todo throw exception
			}

			// send new result communication message
			message = new_result;
			send(sock, &message, sizeof(int32_t), 0);

			// sending client id
			auto client = con.Query("select * from client_information");
			auto client_id = client->Fetch()->GetValue(0, 1);
			send(sock, &client_id, sizeof(uint64_t), 0);

			// send query name
			auto query_name_size = query_name.size();
			send(sock, &query_name_size, sizeof(query_name_size), 0);
			send(sock, query_name.c_str(), query_name_size, 0);

			// now updating last statistics timestamp
			auto timestamp = Timestamp::GetCurrentTimestamp();
			auto r4 = con.Query("update client_information set last_result='" + Timestamp::ToString(timestamp) + "';");

			// also send the timestamp (in case of network delays)
			send(sock, &timestamp, sizeof(int64_t), 0);

			// sending (todo encrypted) data
			SendChunks(result, sock);

			// todo send json here

			// done

			std::cout << "Closing\n";

			// closing the connected socket
			message = close_connection;
			send(sock, &message, sizeof(int32_t), 0);
			close(sock);
			shutdown(sock, SHUT_RDWR);
		}

		// todo refresh the queries eventually

		// calculate the time for the next execution
		std::chrono::system_clock::time_point next =
		    std::chrono::system_clock::now() + std::chrono::hours(query_execution_time_hours);

		// sleep until the next execution time
		std::this_thread::sleep_until(next);
	}
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
