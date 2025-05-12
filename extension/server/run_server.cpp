//
// Created by ila on 2/24/25.
//

#include "include/flush_function.hpp"

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
#include <thread>
#include <mutex>
#include <atomic>

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

void InsertNewClient(int32_t connfd, Connection &con, unordered_map<string, string> &config) {
	uint64_t id;
	uint64_t timestamp_size;

	read(connfd, &id, sizeof(uint64_t));
	read(connfd, &timestamp_size, sizeof(uint64_t));

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

void InsertNewResult(int32_t connfd, Connection &metadata_con, Connection &client_con,
                     unordered_map<string, string> &config) {
	uint64_t id;
	read(connfd, &id, sizeof(uint64_t));

	auto client_info = metadata_con.Query("SELECT * FROM rdda_clients WHERE id = " + to_string(id));
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

	uint64_t timestamp_size;
	read(connfd, &timestamp_size, sizeof(uint64_t));

	vector<char> timestamp(timestamp_size);
	read(connfd, timestamp.data(), timestamp_size);

	idx_t n_chunks;
	read(connfd, &n_chunks, sizeof(idx_t));

	string view_name_str(view_name.begin(), view_name.end());
	string timestamp_str(timestamp.begin(), timestamp.end());

	string centralized_view =
	    (config["schema_name"] != "main" ? config["schema_name"] + "." : "") + "rdda_centralized_view_" + view_name_str;

	auto view_info = client_con.TableInfo(centralized_view);
	if (!view_info) {
		throw ParserException("Centralized view not found: " + centralized_view);
	}

	// we extract the window
	auto window_query =
	    "select rdda_window from rdda_current_window where view_name = 'rdda_centralized_view_" + view_name_str + "';";
	auto r = metadata_con.Query(window_query);
	if (r->HasError() || r->RowCount() == 0) {
		throw ParserException("Error while querying window metadata: " + r->GetError());
	}
	auto window = std::stoi(r->GetValue(0, 0).ToString());

	client_con.BeginTransaction();
	Appender appender(client_con, centralized_view);

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
	client_con.Commit();
	Printer::Print("New result inserted for the view: " + view_name_str + "...");
}

void InsertNewStatistics(int32_t connfd) {
	// Placeholder for future logic
}

void UpdateTimestampClient(int32_t connfd, Connection &con) {
	// note: this assumes a successful insertion of the data
	// to be used only with strong transactional guarantees and a successful commit
	uint64_t id;
	uint64_t timestamp_size;

	read(connfd, &id, sizeof(uint64_t));
	read(connfd, &timestamp_size, sizeof(uint64_t));

	vector<char> timestamp(timestamp_size);
	read(connfd, timestamp.data(), timestamp_size);
	string timestamp_string(timestamp.begin(), timestamp.end());

	con.BeginTransaction();
	con.Query("update rdda_client set last_update = '" + timestamp_string + "' where id = " + std::to_string(id) + ";");
	con.Commit();
}

// Global mutex for thread-safe operations on shared resources
std::mutex client_socket_mutex;
std::mutex db_mutex; // For database operations if needed

void CloseConnection(int connfd, vector<int32_t> &client_socket, int index) {
	std::lock_guard<std::mutex> lock(client_socket_mutex);

	sockaddr_in peeraddr;
	socklen_t addrlen = sizeof(peeraddr);
	getpeername(connfd, reinterpret_cast<struct sockaddr *>(&peeraddr), &addrlen);
	Printer::Print("Host disconnected!");
	close(connfd);
	client_socket[index] = 0;
}

void FlushRemotely(int32_t connfd) {
	// receive the view name (and its size)
	size_t view_name_size;
	read(connfd, &view_name_size, sizeof(size_t));
	vector<char> view_name(view_name_size);
	read(connfd, view_name.data(), view_name_size);
	string view_name_str(view_name.begin(), view_name.end());
	// now also the database name (and its size)
	size_t db_name_size;
	read(connfd, &db_name_size, sizeof(size_t));
	vector<char> db_name(db_name_size);
	read(connfd, db_name.data(), db_name_size);
	string db_name_str(db_name.begin(), db_name.end());
	DuckDB db(nullptr);
	Connection con(db);
	auto r = con.Query("pragma flush('" + view_name_str + "', '" + db_name_str + "');");
	if (r->HasError()) {
		throw ParserException("Error while flushing view: " + r->GetError());
	}
	Printer::Print("Flushed view: " + view_name_str + "...");

}

void UpdateWindowRemotely(int32_t connfd, Connection &con) {
	// receive the view name (and its size)
	size_t view_name_size;
	read(connfd, &view_name_size, sizeof(size_t));
	vector<char> view_name(view_name_size);
	read(connfd, view_name.data(), view_name_size);
	string view_name_str(view_name.begin(), view_name.end());
	// now we update the window
	auto r = con.Query("update rdda_current_window set rdda_window = rdda_window + 1, last_update = now() where view_name = 'rdda_centralized_view_" +
		view_name_str + "';");
	if (r->HasError()) {
		throw ParserException("Error while updating window metadata: " + r->GetError());
	}
	Printer::Print("Updated window for view: " + view_name_str + "...");
}

void ClientThreadHandler(int connfd, unordered_map<string, string> &config, vector<int32_t> &client_socket, int index) {
	while (true) {
		client_messages message;
		ssize_t read_bytes = read(connfd, &message, sizeof(int32_t));

		if (read_bytes <= 0) {
			CloseConnection(connfd, client_socket, index);
			return;
		}

		Printer::Print("Reading message: " + ToString(message) + "...");

		// Each thread gets its own database connections
		string metadata_db_name = "rdda_parser.db";
		string client_db_name = "rdda_client.db";
		DuckDB metadata_db(metadata_db_name);
		DuckDB client_db(client_db_name);
		Connection metadata_con(metadata_db);
		Connection client_con(client_db);

		switch (message) {
		case close_connection:
			CloseConnection(connfd, client_socket, index);
			return;

		case new_client:
			InsertNewClient(connfd, metadata_con, config);
			break;

		case new_result:
			InsertNewResult(connfd, metadata_con, client_con, config);
			break;

		case new_statistics:
			InsertNewStatistics(connfd);
			break;

		case update_timestamp_client:
			UpdateTimestampClient(connfd, metadata_con);
			break;

		case flush:
			FlushRemotely(connfd);
			break;

		case update_window:
			UpdateWindowRemotely(connfd, metadata_con);
			break;

		default:
			Printer::Print("Unknown message received: " + ToString(message));
			CloseConnection(connfd, client_socket, index);
			break;
		}
	}
}

// Main server function
void RunServer(ClientContext &context, const FunctionParameters &parameters) {
	server_running = true;
	SetupSignalHandling();

	// Ignore SIGPIPE to prevent crashing when writing to closed sockets
	std::signal(SIGPIPE, SIG_IGN);

	string config_path = "../extension/server/";
	string config_file = "server.config";
	auto config = ParseConfig(config_path, config_file);

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

	// Enable forced close on lingering sockets
	struct linger sl;
	sl.l_onoff = 1;   // Enable linger option
	sl.l_linger = 0;  // Discard pending data, close immediately
	if (setsockopt(sockfd, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl))) {
		Printer::Print("setsockopt(SO_LINGER) failed");
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
	vector<std::thread> client_threads;
	socklen_t addrlen = sizeof(servaddr);

	try {
		while (server_running) {
			fd_set readfds;
			FD_ZERO(&readfds);

			FD_SET(sockfd, &readfds);
			FD_SET(self_pipe[0], &readfds);

			int32_t max_sockfd = std::max(sockfd, self_pipe[0]);

			// Clean up finished threads
			for (auto it = client_threads.begin(); it != client_threads.end();) {
				if (it->joinable()) {
					it->join();
					it = client_threads.erase(it);
				} else {
					++it;
				}
			}

			struct timeval timeout;
			timeout.tv_sec = 5; // Check server_running every 5 seconds
			timeout.tv_usec = 0;

			int activity = select(max_sockfd + 1, &readfds, nullptr, nullptr, &timeout);

			if (activity < 0 && errno != EINTR) {
				string error_message = "Select error: " + std::to_string(errno);
				Printer::Print(error_message);
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

				// Find empty slot for new client
				std::lock_guard<std::mutex> lock(client_socket_mutex);
				for (int i = 0; i < max_clients; i++) {
					if (client_socket[i] == 0) {
						client_socket[i] = connfd;

						// Launch a new thread for this client
						client_threads.emplace_back([connfd, &config, &client_socket, i]() {
							ClientThreadHandler(connfd, config, client_socket, i);
						});

						break;
					}
				}
			}
		}
	}
	catch (const std::exception &e) {
        Printer::Print("Interrupting execution and cleaning up sockets.");
    }

	// Wait for all client threads to finish
	for (auto &thread : client_threads) {
		if (thread.joinable()) {
			thread.join();
		}
	}

	CleanupSockets(sockfd, client_socket);
	Printer::Print("Server shut down cleanly.");
}

} // namespace duckdb
