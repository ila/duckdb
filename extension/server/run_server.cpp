//
// Created by ila on 2/24/25.
//

#include "include/flush_function.hpp"

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/printer.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/planner.hpp>
#include "duckdb/planner/binder.hpp"

#include <common.hpp>
#include <compiler_extension.hpp>
#include <logical_plan_to_string.hpp>
#include <regex>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/buffered_file_reader.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/function/aggregate/distributive_functions.hpp>
#include <duckdb/main/database.hpp>
#include <netinet/in.h>
#include <csignal>

namespace duckdb {

std::atomic<bool> running{true};

void SignalHandler(int signal) {
	running = false;
}

void SerializeQueryPlan(string &query, string &path, string &dbname) {

	DuckDB db(path + dbname);
	Connection con(db);

	db.GetFileSystem();

	// overwriting the same file
	BufferedFileWriter target(db.GetFileSystem(), path + "rdda_serialized_plan.binary");
	// serializer.SetVersion(PLAN_SERIALIZATION_VERSION);
	// serializer.Write(serializer.GetVersion());

	con.BeginTransaction();
	Parser p;
	p.ParseQuery(query);

	Planner planner(*con.context);

	planner.CreatePlan(move(p.statements[0]));
	auto plan = move(planner.plan);

	Optimizer optimizer(*planner.binder, *con.context);
	plan = optimizer.Optimize(move(plan));

	BinarySerializer serializer(target);
	serializer.Begin();
	plan->Serialize(serializer);
	serializer.End();

	con.Rollback();

	target.Sync();
}

void DeserializeQueryPlan(string &path, string &dbname) {

	string file_path = path + "rdda_serialized_plan.binary";

	DuckDB db(path + dbname);
	Connection con(db);

	BufferedFileReader file_source(db.GetFileSystem(), file_path.c_str());

	con.Query("PRAGMA enable_profiling=json");
	con.Query("PRAGMA profile_output='profile_output.json'");

	BinaryDeserializer deserializer(file_source);
	deserializer.Set<ClientContext &>(*con.context);
	deserializer.Begin();
	auto plan = LogicalOperator::Deserialize(deserializer);
	deserializer.End();

	plan->Print();
	plan->ResolveOperatorTypes();

	//auto statement = make_uniq<LogicalPlanStatement>(move(plan));
	//auto result = con.Query(move(statement)); todo
	//result->Print();
}

void InsertClient(Connection &con, unordered_map<string, string> &config, uint64_t id,
						 string &timestamp) {

	string table_name = "rdda_clients";
	string query = "insert or ignore into " + table_name + " values (" + std::to_string(id) + ", '" + timestamp + "', NULL);";
	auto r = con.Query(query);
	if (r->HasError()) {
		throw ParserException("Error while inserting client information: " + r->GetError());
	}
	Printer::Print("Added client: " + std::to_string(id));
}

void RunServer(ClientContext &context, const FunctionParameters &parameters) {

	// Initialize configuration and database connection
	string config_path = "../extension/server/";;
	string config_file = "server.config";
	auto config = ParseConfig(config_path, config_file);
	Connection con(*context.db);

	// Parse configuration
	int32_t max_clients = std::stoi(config["max_clients"]);
	int32_t server_port = std::stoi(config["server_port"]);

	// Prepare socket structures and variables
	sockaddr_in servaddr;
	memset(&servaddr, 0, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = INADDR_ANY;
	servaddr.sin_port = htons(server_port);

	int32_t sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (sockfd == -1) {
		Printer::Print("Socket creation not successful");
		return;
	}

	// Set socket options for address reuse
	int opt = 1;
	if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
		Printer::Print("setsockopt failed");
		exit(EXIT_FAILURE);
	}

	// Bind and listen
	if (::bind(sockfd, reinterpret_cast<struct sockaddr *>(&servaddr), sizeof(servaddr)) != 0) {
		Printer::Print("Bind not successful!");
		return;
	}
	if (listen(sockfd, 10) != 0) {
		Printer::Print("Listen not successful!");
		return;
	}
	Printer::Print("Ready to accept connections!");

	// Initialize client socket array
	vector<int32_t> client_socket(max_clients, 0);
	fd_set readfds;
	socklen_t addrlen = sizeof(servaddr);
	int32_t activity, connfd, socket_descriptor, max_sockfd;

	while (true) {
		// Clear and prepare the file descriptor set
		FD_ZERO(&readfds);
		FD_SET(sockfd, &readfds);
		max_sockfd = sockfd;

		for (int i = 0; i < max_clients; i++) {
			socket_descriptor = client_socket[i];
			if (socket_descriptor > 0) {
				FD_SET(socket_descriptor, &readfds);
			}
			if (socket_descriptor > max_sockfd) {
				max_sockfd = socket_descriptor;
			}
		}

		// Wait indefinitely for activity on one of the sockets
		struct timeval timeout;
		timeout.tv_sec = 86400; // 24 hours
		timeout.tv_usec = 0;
		activity = select(max_sockfd + 1, &readfds, nullptr, nullptr, &timeout);
		if ((activity < 0) && (errno != EINTR)) {
			Printer::Print("Select error!");
		}

		// Accept new connection if there is one
		if (FD_ISSET(sockfd, &readfds)) {
			Printer::Print("Incoming connection!");
			connfd = accept(sockfd, reinterpret_cast<struct sockaddr *>(&servaddr), &addrlen);
			if (connfd < 0) {
				Printer::Print("Connection error!");
				continue;
			}
			// Add new socket to client array
			for (int i = 0; i < max_clients; i++) {
				if (client_socket[i] == 0) {
					client_socket[i] = connfd;
					break;
				}
			}
		}

		// Process I/O on each client socket
		for (int i = 0; i < max_clients; i++) {
			connfd = client_socket[i];
			if (connfd == 0) {
				continue;
			}

			if (FD_ISSET(connfd, &readfds)) {
				client_messages message;
				ssize_t read_bytes = read(connfd, &message, sizeof(int32_t));
				if (read_bytes <= 0) {
					// Assume connection closed or error
					close(connfd);
					client_socket[i] = 0;
					continue;
				}
				Printer::Print("Reading message: " + toString(message) + "...");

				switch (message) {
				case close_connection: {
					getpeername(connfd, reinterpret_cast<struct sockaddr *>(&servaddr), &addrlen);
					Printer::Print("Host disconnected!");
					close(connfd);
					client_socket[i] = 0;
					break;
				}
				case new_client: {
					// Receive client information
					uint64_t id;
					size_t timestamp_size;

					read(connfd, &id, sizeof(uint64_t));
					read(connfd, &timestamp_size, sizeof(size_t));
					vector<char> timestamp(timestamp_size);
					read(connfd, &timestamp[0], timestamp_size);
					string timestamp_string(timestamp.begin(), timestamp.end());
					InsertClient(con, config, id, timestamp_string);

					// Read SQL instructions and send them to the client
					// first check if the file exists
					auto file_name = "decentralized_queries.sql";
					auto file_path = config["db_path"] + file_name;
					auto queries = CompilerExtension::ReadFile("decentralized_queries.sql");
					if (queries.empty()) {
						throw ParserException("File does not exist: " + file_path);
					}
					// Send the file to the client
					auto size = queries.size();
					send(connfd, &size, sizeof(size_t), 0);
					send(connfd, queries.c_str(), queries.size(), 0);\
					Printer::Print("Sent queries to client!");
					break;
				}
				case new_result: {
					// Receive client id, view name and timestamp
					Value id;
					read(connfd, &id, sizeof(uint64_t));

					size_t view_name_size;
					read(connfd, &view_name_size, sizeof(size_t));
					vector<char> view_name(view_name_size);
					read(connfd, view_name.data(), view_name_size);

					int64_t timestamp;
					read(connfd, &timestamp, sizeof(int64_t));

					idx_t n_chunks;
					read(connfd, &n_chunks, sizeof(idx_t));
					Printer::Print("Reading chunks!");

					string view_name_string(view_name.begin(), view_name.end());
					string centralized_view_name;
					if (config["schema_name"] != "main") {
						centralized_view_name = config["schema_name"] + ".";
					}
					centralized_view_name += "rdda_centralized_view_" + view_name_string;

					auto view_info = con.TableInfo(centralized_view_name);
					Appender appender(con, centralized_view_name);

					for (idx_t j = 0; j < n_chunks; j++) {
						ssize_t chunk_len;
						read(connfd, &chunk_len, sizeof(ssize_t));

						vector<char> buffer(chunk_len);
						read(connfd, buffer.data(), chunk_len);

						MemoryStream stream(reinterpret_cast<data_ptr_t>(buffer.data()), chunk_len);
						BinaryDeserializer deserializer(stream);
						DataChunk chunk;
						chunk.Deserialize(deserializer);

						if (view_info) {
							for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
								appender.BeginRow();
								for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
									Value value = chunk.GetValue(col_idx, row_idx);
									appender.Append(value);
								}
								// Append client id and timestamp
								appender.Append(id);
								appender.Append(timestamp);
								appender.EndRow();
							}
						} else {
							throw ParserException("Centralized view not found: " + centralized_view_name);
						}
						// now update the last_update
						auto update = "insert into rdda_client_refreshes values ('" + view_name_string + "', '" + std::to_string(timestamp) + "') on conflict do update set last_result = '" + std::to_string(timestamp) + "' where view_name = '" + view_name_string + "';";
						auto r = con.Query(update);
						if (r->HasError()) {
							throw ParserException("Error while updating last result: " + r->GetError());
						}
					}
					break;
				}
				case new_statistics: {
					// TODO: Receive and process JSON statistics
					break;
				}
				default: {
					Printer::Print("Unknown message received");
					break;
				}
				}
			}
		}
	} // End while(true)
	Printer::Print("Server shutting down...");
}
}