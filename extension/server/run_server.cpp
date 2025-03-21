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
#include <duckdb/common/types/time.hpp>

namespace std {
class exception;
}
namespace duckdb {

// Global state for server shutdown
volatile sig_atomic_t server_running = true;

// Self-pipe for waking up select() after receiving a signal
int self_pipe[2];

void SignalInsertr(int signal) {
	if (signal == SIGINT || signal == SIGTERM) {
		Printer::Print("\nReceived shutdown signal, stopping server...");
		server_running = false;

		// Write to self-pipe to wake up select()
		char buf = 1;
		write(self_pipe[1], &buf, 1);
	}
}

void SetupSignalHandling() {
	signal(SIGINT, SignalInsertr);
	signal(SIGTERM, SignalInsertr);
}

void CleanupSockets(int sockfd, vector<int32_t> &client_socket) {
	close(sockfd);
	for (auto fd : client_socket) {
		if (fd > 0) {
			close(fd);
		}
	}
	close(self_pipe[0]);
	close(self_pipe[1]);
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

	// auto statement = make_uniq<LogicalPlanStatement>(move(plan));
	// auto result = con.Query(move(statement)); todo
	// result->Print();
}

void InsertClient(Connection &con, unordered_map<string, string> &config, uint64_t id, string &timestamp) {

	string table_name = "rdda_clients";
	string query = "insert or ignore into " + table_name + " values (" + std::to_string(id) + ", '" + timestamp +
	               "', '" + timestamp + "');";
	auto r = con.Query(query);
	if (r->HasError()) {
		throw ParserException("Error while inserting client information: " + r->GetError());
	}
	Printer::Print("Added client: " + std::to_string(id));
}

void CloseConnection(int connfd, vector<int32_t> &client_socket, int index) {
	sockaddr_in peeraddr;
	socklen_t addrlen = sizeof(peeraddr);
	getpeername(connfd, reinterpret_cast<struct sockaddr *>(&peeraddr), &addrlen);
	Printer::Print("Host disconnected!");
	close(connfd);
	client_socket[index] = 0;
}

void InsertNewClient(int32_t connfd, Connection &con, unordered_map<string, string> &config) {
	uint64_t id;
	size_t timestamp_size;

	read(connfd, &id, sizeof(uint64_t));
	read(connfd, &timestamp_size, sizeof(size_t));

	vector<char> timestamp(timestamp_size);
	read(connfd, timestamp.data(), timestamp_size);
	string timestamp_string(timestamp.begin(), timestamp.end());

	InsertClient(con, config, id, timestamp_string);

	string file_name = "decentralized_queries.sql";
	string file_path = config["db_path"] + file_name;
	auto queries = CompilerExtension::ReadFile(file_name);

	if (queries.empty()) {
		throw ParserException("Missing query file: " + file_path);
	}

	size_t size = queries.size();
	send(connfd, &size, sizeof(size_t), 0);
	send(connfd, queries.c_str(), size, 0);
	Printer::Print("Sent queries to client!");
}

void InsertNewResult(int32_t connfd, Connection &con, unordered_map<string, string> &config) {
	uint64_t id;
	read(connfd, &id, sizeof(uint64_t));

	auto client_info = con.Query("SELECT * FROM rdda_clients WHERE id = " + to_string(id));
	if (client_info->RowCount() == 0) {
		int32_t error = error_non_existing_client;
		send(connfd, &error, sizeof(int32_t), 0);
		return;
	}

	int32_t ok_message = ok;
	send(connfd, &ok_message, sizeof(int32_t), 0);

	size_t view_name_size;
	read(connfd, &view_name_size, sizeof(size_t));

	vector<char> view_name(view_name_size);
	read(connfd, view_name.data(), view_name_size);

	size_t timestamp_size;
	read(connfd, &timestamp_size, sizeof(size_t));

	vector<char> timestamp(timestamp_size);
	read(connfd, timestamp.data(), timestamp_size);

	idx_t n_chunks;
	read(connfd, &n_chunks, sizeof(idx_t));

	string view_name_str(view_name.begin(), view_name.end());
	string timestamp_str(timestamp.begin(), timestamp.end());

	string centralized_view =
	    (config["schema_name"] != "main" ? config["schema_name"] + "." : "") + "rdda_centralized_view_" + view_name_str;

	auto view_info = con.TableInfo(centralized_view);
	if (!view_info) {
		throw ParserException("Centralized view not found: " + centralized_view);
	}

	// we extract the window
	auto window_query =
	    "select rdda_window from rdda_current_window where view_name = 'rdda_centralized_view_" + view_name_str + "';";
	auto r = con.Query(window_query);
	if (r->HasError() || r->RowCount() == 0) {
		throw ParserException("Error while querying window metadata: " + r->GetError());
	}
	auto window = std::stoi(r->GetValue(0, 0).ToString());

	con.BeginTransaction();
	Appender appender(con, centralized_view);

	auto ts_gen = Timestamp::FromString(timestamp_str);
	auto ts_arr = Timestamp::GetCurrentTimestamp();

	for (idx_t i = 0; i < n_chunks; i++) {
		ssize_t chunk_len;
		read(connfd, &chunk_len, sizeof(ssize_t));

		vector<char> buffer(chunk_len);
		read(connfd, buffer.data(), chunk_len);

		MemoryStream stream((data_ptr_t)buffer.data(), chunk_len);
		BinaryDeserializer deserializer(stream);
		DataChunk chunk;
		chunk.Deserialize(deserializer);

		for (idx_t row = 0; row < chunk.size(); row++) {
			appender.BeginRow();
			for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
				appender.Append(chunk.GetValue(col, row));
			}
			appender.Append(ts_gen);
			appender.Append(ts_arr);
			appender.Append<int32_t>(window);
			appender.Append<uint64_t>(id);
			appender.Append<int8_t>(1); // todo - change the action
			appender.EndRow();
		}
	}
	appender.Close();
	con.Commit();
	Printer::Print("New result inserted for the view " + view_name_str);
}

void InsertNewStatistics(int32_t connfd) {
	// Placeholder for future logic
}

void HandleClientMessage(client_messages message, int32_t connfd, Connection &con,
                         unordered_map<string, string> &config, vector<int32_t> &client_socket, int index) {
	switch (message) {
	case close_connection:
		CloseConnection(connfd, client_socket, index);
		break;

	case new_client:
		InsertNewClient(connfd, con, config);
		break;

	case new_result:
		InsertNewResult(connfd, con, config);
		break;

	case new_statistics:
		InsertNewStatistics(connfd);
		break;

	default:
		Printer::Print("Unknown message received");
		break;
	}
}

// Main server function
void RunServer(ClientContext &context, const FunctionParameters &parameters) {
	server_running = true;
	SetupSignalHandling();

	string config_path = "../extension/server/";
	string config_file = "server.config";
	auto config = ParseConfig(config_path, config_file);
	Connection con(*context.db);

	int32_t max_clients = std::stoi(config["max_clients"]);
	int32_t server_port = std::stoi(config["server_port"]);

	sockaddr_in servaddr {};
	memset(&servaddr, 0, sizeof(servaddr));
	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = INADDR_ANY;
	servaddr.sin_port = htons(server_port);

	int32_t sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (sockfd == -1) {
		Printer::Print("Socket creation not successful");
		return;
	}

	int opt = 1;
	if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
		Printer::Print("setsockopt failed");
		close(sockfd);
		return;
	}

	if (bind(sockfd, reinterpret_cast<struct sockaddr *>(&servaddr), sizeof(servaddr)) != 0) {
		Printer::Print("Bind not successful!");
		close(sockfd);
		return;
	}

	if (listen(sockfd, 10) != 0) {
		Printer::Print("Listen not successful!");
		close(sockfd);
		return;
	}

	Printer::Print("Ready to accept connections!");

	// Initialize self-pipe
	if (pipe(self_pipe) == -1) {
		Printer::Print("Failed to create self-pipe!");
		close(sockfd);
		return;
	}

	vector<int32_t> client_socket(max_clients, 0);
	socklen_t addrlen = sizeof(servaddr);

	while (server_running) {
		fd_set readfds;
		FD_ZERO(&readfds);

		FD_SET(sockfd, &readfds);
		FD_SET(self_pipe[0], &readfds);

		int32_t max_sockfd = std::max(sockfd, self_pipe[0]);

		for (int i = 0; i < max_clients; i++) {
			if (client_socket[i] > 0) {
				FD_SET(client_socket[i], &readfds);
				max_sockfd = std::max(max_sockfd, client_socket[i]);
			}
		}

		struct timeval timeout;
		timeout.tv_sec = 5; // Check server_running every 5 seconds
		timeout.tv_usec = 0;

		int activity = select(max_sockfd + 1, &readfds, nullptr, nullptr, &timeout);

		if (activity < 0 && errno != EINTR) {
			Printer::Print("Select error!");
			break;
		}

		if (!server_running)
			break;

		if (FD_ISSET(self_pipe[0], &readfds)) {
			char buf;
			read(self_pipe[0], &buf, 1);
			break; // Exit after reading self-pipe
		}

		if (FD_ISSET(sockfd, &readfds)) {
			int connfd = accept(sockfd, reinterpret_cast<struct sockaddr *>(&servaddr), &addrlen);
			if (connfd < 0) {
				Printer::Print("Connection error!");
				continue;
			}

			for (int i = 0; i < max_clients; i++) {
				if (client_socket[i] == 0) {
					client_socket[i] = connfd;
					break;
				}
			}
		}

		for (int i = 0; i < max_clients; i++) {
			int connfd = client_socket[i];
			if (connfd <= 0)
				continue;

			if (FD_ISSET(connfd, &readfds)) {
				client_messages message;
				ssize_t read_bytes = read(connfd, &message, sizeof(int32_t));
				if (read_bytes <= 0) {
					close(connfd);
					client_socket[i] = 0;
					continue;
				}

				Printer::Print("Reading message: " + toString(message) + "...");
				HandleClientMessage(message, connfd, con, config, client_socket, i);
			}
		}
	}

	CleanupSockets(sockfd, client_socket);
	Printer::Print("Server shut down cleanly.");
}

} // namespace duckdb
