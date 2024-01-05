#define DUCKDB_EXTENSION_MAIN

#include "server_extension.hpp"

#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/planner.hpp"

#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <regex>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

// TODO cleanup unused libraries
#include <arpa/inet.h>
#include <map>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace duckdb {

/*
int32_t ResponseRatio() {
    // every insertion also triggers an update of the last update timestamp
}

int32_t MinimumResponse() {
    // every insertion also triggers an update of the last update timestamp
} */

static void Flush(Connection &con, string &query_name) {
	// calling this function on centralized views
	// two parameters: window and TTL
	// window = maximum number of windows that should be kept in the decentralized view before copying
	// ttl = maximum time-to-live of a record [hours]
	// we also need minimum aggregation (minimum number of elements in a group)

	// assuming the data here is decrypted, let's check if the minimum aggregation is met
	// find the columns with minimum aggregation

	// data looks like this:
	// | field1 | field2 | ... | grouping_key | client_id | timestamp |

	// the grouping key is the column with maximum minimum_aggregation

	string centralized_table_name = "rdda_centralized_table_" + query_name;
	auto table_info = con.TableInfo(centralized_table_name); // todo test schema

	// extract window and ttl of the view
	auto window_ttl = con.Query(
	    "select window, ttl from rdda_view_constraints where view_name = 'rdda_centralized_view_" + query_name + "';");
	// todo exception handling
	auto window = window_ttl->Fetch()->GetValue(0, 1).GetValue<uint8_t>();
	auto ttl = window_ttl->Fetch()->GetValue(1, 1).GetValue<uint8_t>();

	// SELECT *, CEIL(EXTRACT(EPOCH FROM timestamp) / (ttl * 3600)) AS window FROM view WHERE window <= n_windows;\

	// now check if minimum granularity is met and either discard or insert into a centralized view
	// extracting minimum granularity (if more than one column in the table has it, select the max)
	// todo check the column exists in the query
	auto result = con.Query("select column, max(minimum aggregation) "
	                        "from rdda_table_constraints "
	                        "where table in (select tables from rdda_queries) "
	                        "and minimum_aggregation > 0 "
	                        "group by column order by max;");
	auto column_name = result->Fetch()->GetValue(0, 1).GetValue<string>(); // todo check this
	auto granularity = result->Fetch()->GetValue(1, 1).GetValue<uint8_t>();

	if (window > 0 && ttl > 0) {
		if (granularity > 0) {
			// divide the data in window and check if any tuple is out of scope
			// window needs to be recalculated every time, so can't be permanently stored
			// todo make sure the timestamp field is properly named
			// extracting all the tuples out of scope
			// I am honestly ashamed of this code
			string query_in = "WITH tuples_out_of_scope as "
			                  "(SELECT *, CEIL(EXTRACT(EPOCH FROM timestamp) / " +
			                  std::to_string(ttl) +
			                  " * 3600)) "
			                  "AS window FROM rdda_centralized_view_" +
			                  query_name + " WHERE window > " + std::to_string(window) +
			                  ") "
			                  "select * from tuples_out_of_scope "
			                  "where " +
			                  column_name + " in (select " + column_name +
			                  ", count(*) "
			                  "from tuples_out_of_scope group by " +
			                  column_name + "having count >= " + std::to_string(granularity) + ");";

			auto result_in = con.Query(query_in);
			auto &collection = result_in->Collection();
			idx_t num_chunks = collection.ChunkCount();

			for (auto &chunk : collection.Chunks()) {
				con.Append(*table_info, chunk);
			}

			// now remove all the windows out of scope
			string query_out = "delete from rdda_decentralized_view_" + query_name +
			                   " where rowid in "
			                   "(select rowid from (SELECT rowid, CEIL(EXTRACT(EPOCH FROM timestamp) / " +
			                   std::to_string(ttl) +
			                   " * 3600)) "
			                   "AS window FROM rdda_centralized_view_\" + query_name +\n"
			                   " WHERE window > \" + std::to_string(window) + \"))";
			con.Query(query_out);
		}
	} else if (window == 0 && ttl > 0) {
		// just flush expired tuples
		if (granularity > 0) { // todo null?
			string query_insert = "with tuples_out_of_scope as"
			                      "(select * from rdda_centralized_view_" +
			                      query_name + " where timestamp < (current_time - interval " + std::to_string(ttl) +
			                      " hours))"
			                      "select * from tuples_out_of_scope where column in "
			                      "(select " +
			                      column_name + ", count(*) from tuples_out_of_scope group by " + column_name +
			                      " having count >= " + std::to_string(granularity) + ");";
			auto tuples_ok = con.Query(query_insert);
			auto &collection = tuples_ok->Collection();
			idx_t num_chunks = collection.ChunkCount();

			for (auto &chunk : collection.Chunks()) {
				con.Append(*table_info, chunk);
			}
			// now remove everything from the query
			string query_delete = "delete from rdda_centralized_view_" + query_name +
			                      " where rowid in (select rowid from rdda_centralized_view_" + query_name +
			                      " where timestamp < (current_time - interval " + std::to_string(ttl) + " hours));";
			con.Query(query_delete);
		} else {
			// no granularity specified, just flush everything out of scope
			auto tuples = con.Query("select * from " + query_name + " where timestamp >= (current_time - interval " +
			                        std::to_string(ttl) + " hours));");
			auto &collection = tuples->Collection();
			idx_t num_chunks = collection.ChunkCount();

			for (auto &chunk : collection.Chunks()) {
				con.Append(*table_info, chunk);
			}
			con.Query("delete from rdda_centralized_view_" + query_name +
			          " where timestamp < (current_time - interval " + std::to_string(ttl) + " hours));");
		}
	} // all the other cases are not possible in our architecture
}

static void CreateViewFromCSV(string &view_name, string &csv_path, Connection &con) {

	// we write the chunks to a csv and use the auto-detect function to create the view
	string query_string = "create table " + view_name + " as select * from read_csv_auto('" + csv_path + "');";
	con.Query(query_string);

	std::remove(csv_path.c_str());
}

static void GenerateQueryPlan(string &query, string &path, string &dbname) {

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

static void DeserializeQueryPlan(string &path, string &dbname) {

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

	auto statement = make_uniq<LogicalPlanStatement>(move(plan));
	auto result = con.Query(move(statement));
	result->Print();
}

static void InsertClient(Connection &con, unordered_map<string, string> &config, uint64_t id, string &timestamp) {

	string table_name;
	if (config["schema_name"] != "main") {
		table_name = config["schema_name"] + ".clients";
	} else {
		table_name = "clients";
	}
	Appender appender(con, table_name);
	appender.AppendRow(id, timestamp);
}

static void InitializeServer(string &config_path, unordered_map<string, string> &config) {

	DuckDB db(config["db_path"] + config["db_name"]);
	Connection con(db);

	CreateSystemTables(config_path, config["db_path"], config["db_name"], config["schema_name"], con);

	for (auto &config_item : config) {
		string string_insert = "insert into settings values (" + config_item.first + ", " + config_item.second + ");";
		con.Query(string_insert);
	}
}

static void ParseJSON(Connection &con, std::unordered_map<string, string> &config, int32_t connfd, hugeint_t client) {

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
	appender.Append<string>(string(buffer.begin(), buffer.end()));
	appender.Append<hugeint_t>(client);
	appender.Append<timestamp_t>(Timestamp::GetCurrentTimestamp());
	appender.EndRow();
}

static void LoadInternal(DatabaseInstance &instance) {

	// todo:
	// more statistics - volume of data?
	// chunk cardinality?

	// todo for later:
	// how do the clients know the server address?
	// changes to sql
	// aggregated execution
	// compression
	// encryption
	// backups

	string config_path = "/home/ila/Code/duckdb/extension/server/";
	string config_file = "server.config";

	// reading config args
	// todo path is still hardcoded, fix this
	auto config = ParseConfig(config_path, config_file);

	DuckDB db(config["db_path"] + config["db_name"]);
	Connection con(db);

	auto client_info = con.TableInfo(config["schema_name"], "clients");
	if (!client_info) {
		// table does not exist --> the database should be initialized
		std::cout << "Initializing server\n";
		InitializeServer(config_path, config);
	}

	// add a parser extension
	// will probably break, let's see
	auto &db_config = duckdb::DBConfig::GetConfig(instance);
	auto rdda_parser = duckdb::RDDAParserExtension();
	// db.LoadExtension<RDDAParserExtension>();
	rdda_parser.path = config["db_path"];
	rdda_parser.db = config["db_name"];

	db_config.parser_extensions.push_back(rdda_parser);

	// initialize server sockets

	/*
	struct sockaddr_in servaddr;
	bzero(&servaddr, sizeof(servaddr));

	int32_t max_clients = stoi(config["max_clients"]);
	uint32_t activity, connfd, sockfd, max_sockfd, socket_descriptor, opt;
	std::vector<uint32_t> client_socket(max_clients);

	// socket descriptors
	fd_set readfds;
	sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

	for (int i = 0; i < max_clients; i++) {
	    client_socket[i] = 0;
	}

	if (sockfd == -1) {
	    std::cout << "Socket creation not successful\n";
	}

	if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
	    perror("setsockopt");
	    exit(EXIT_FAILURE);
	}

	servaddr.sin_family = AF_INET;
	servaddr.sin_addr.s_addr = INADDR_ANY;
	servaddr.sin_port = htons(stoi(config["server_port"]));

	if (::bind(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) != 0) {
	    std::cout << "Bind not successful\n";
	}
	if ((listen(sockfd, 10)) != 0) {
	    std::cout << "Listen not successful\n";
	};

	std::cout << "Ready to accept connections\n";
	socklen_t l = sizeof(servaddr);

	while (true) {
	    // clear the socket set during startup
	    FD_ZERO(&readfds);

	    // add master socket to set
	    FD_SET(sockfd, &readfds);
	    max_sockfd = sockfd;

	    // add child sockets to set
	    for (int i = 0; i < max_clients; i++) {
	        // create socket descriptor and add it to the read list
	        socket_descriptor = client_socket[i];
	        if (socket_descriptor > 0) {
	            FD_SET(socket_descriptor, &readfds);
	        }

	        if (socket_descriptor > max_sockfd) {
	            max_sockfd = socket_descriptor;
	        }
	    }

	    // wait indefinitely for an activity on one of the sockets
	    activity = select(max_sockfd + 1, &readfds, NULL, NULL, NULL);

	    if ((activity < 0) && (errno != EINTR)) {
	        printf("Select error");
	    }

	    // incoming connection
	    if (FD_ISSET(sockfd, &readfds)) {
	        std::cout << "Hello!\n";
	        connfd = accept(sockfd, (struct sockaddr *)&servaddr, &l);
	        if (connfd < 0) {
	            std::cout << "Connection error!\n";
	        };

	        // inform user of socket number - used in send and receive commands
	        printf("New connection, connfd is %d, IP is %s, port %d\n", connfd, inet_ntoa(servaddr.sin_addr),
	               ntohs(servaddr.sin_port));
	    }

	    // add new socket to array of sockets
	    for (int i = 0; i < max_clients; i++) {
	        // if position is empty
	        if (client_socket[i] == 0) {
	            client_socket[i] = connfd;
	            printf("Adding to list of sockets as %d, connfd %d\n", i, connfd);
	            break;
	        }
	    }

	    // I/O operation on some other socket
	    for (int i = 0; i < max_clients; i++) {

	        connfd = client_socket[i];
	        std::cout << "Connfd " << connfd << "\n";

	        if (FD_ISSET(connfd, &readfds)) {
	            std::cout << "File descriptor set " << connfd << "\n";
	            // read the connection code sent by the client
	            client_messages message;
	            read(connfd, &message, sizeof(int32_t));
	            std::cout << "Read message: " << message << "\n";

	            // check if it exists or the connection is being closed
	            switch (message) {

	            case (close_connection): {
	                // somebody disconnected, get details and print
	                getpeername(connfd, (struct sockaddr *)&servaddr, (socklen_t *)&l);
	                printf("Host disconnected, ip %s, port %d \n", inet_ntoa(servaddr.sin_addr),
	                       ntohs(servaddr.sin_port));

	                // close the socket and mark as 0 in list for reuse
	                close(connfd);
	                client_socket[i] = 0;
	            } break;

	            case (new_client): {
	                // receive the information and store it in the master table
	                // we assume we know the lengths of fields here (uint64_t and timestamp with 23 chars)
	                uint64_t id;
	                string timestamp;
	                read(connfd, &id, sizeof(uint64_t));
	                read(connfd, &timestamp, 23);

	                InsertClient(con, config, id, timestamp);
	            } break;

	            case (new_result): {

	                // receiving client id, query id and timestamp
	                Value id;
	                read(connfd, &id, sizeof(uint64_t));

	                size_t query_id_size;
	                read(connfd, &query_id_size, sizeof(size_t));
	                std::vector<char> query_id(query_id_size);
	                read(connfd, query_id.data(), query_id_size);

	                int64_t timestamp;
	                read(connfd, &timestamp, sizeof(int64_t));

	                idx_t n_chunks;
	                read(connfd, &n_chunks, sizeof(idx_t));
	                std::cout << "Reading chunks: " << n_chunks << "\n" << std::flush;

	                string query_id_string = string(query_id.begin(), query_id.end());
	                string centralized_view_name;
	                if (config["schema_name"] != "main") {
	                    centralized_view_name = centralized_view_name + config["schema_name"] + ".";
	                }
	                centralized_view_name = centralized_view_name + "rdda_centralized_view_" + query_id_string;
	                auto view_info = con.TableInfo(centralized_view_name);
	                Appender appender(con, centralized_view_name); // todo what happens here if the view doesn't exist?
	                auto csv_path = config_path + query_id_string + ".csv";

	                for (idx_t j = 0; j < n_chunks; j++) {

	                    ssize_t chunk_len;
	                    read(connfd, &chunk_len, sizeof(ssize_t));
	                    std::cout << "Reading chunk len " << chunk_len << "\n";

	                    std::vector<char> buffer(chunk_len);
	                    read(connfd, buffer.data(), chunk_len);
	                    std::cout << "Reading chunk\n";

	                    BufferedDeserializer deserializer((data_ptr_t)buffer.data(), chunk_len);
	                    DataChunk chunk;
	                    std::cout << "Deserializing chunk\n";
	                    chunk.Deserialize(deserializer);

	                    if (view_info) {
	                        // the view exists, just append

	                        for (idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
	                            // access values in each column for the current row
	                            appender.BeginRow();
	                            for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
	                                Value value = chunk.GetValue(col_idx, row_idx);
	                                appender.Append(value);
	                            }
	                            // also append client id and timestamp
	                            appender.Append(id);
	                            appender.Append(timestamp);
	                            appender.EndRow();
	                        }

	                    } else {
	                        // throw an error todo
	                    }
	                }
	            } break;

	            case (new_statistics): {
	                // receive json statistics
	                // todo

	            } break;
	            }
	        }
	    }
	} */

	// check if the window has elapsed
}

void ServerExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string ServerExtension::Name() {
	return "server";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void server_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *server_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
