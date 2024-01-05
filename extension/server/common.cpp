#include "include/common.hpp"

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/planner.hpp"

#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <regex>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <string>

namespace duckdb {

unordered_map<string, string> ParseConfig(string &path, string &config_name) {

	unordered_map<string, string> config;
	std::ifstream config_file(path + config_name);
	string line;

	while (getline(config_file, line)) {
		std::istringstream config_line(line);
		string key;
		if (std::getline(config_line, key, '=')) {
			string value;
			if (std::getline(config_line, value)) {
				// todo strip strings from whitespaces
				config.insert(make_pair(key, value));
			}
		}
	}
	// todo exception handling
	return config;
}

void CreateSystemTables(string &path, string &db_path, string &db_name, string &schema_name, Connection &con) {

	// there is currently no way to execute SQL statements in files through the C++ API
	// so here we go again...
	std::ifstream input_file(path + "tables.sql");
	string query;

	if (schema_name != "main") {
		// try to recreate the schema
		con.Query("create schema if not exists " + schema_name);
	}

	while (getline(input_file, query)) {
		std::istringstream config_line(query);
		if (schema_name != "main") {
			query = std::regex_replace(query, std::regex("create table "), "create table " + schema_name + ".");
		}
		con.BeginTransaction();
		auto result = con.Query(query);
		con.Commit();
	}
}

void SendFile(std::unordered_map<string, string> &config, int32_t sock) {
	// read file in memory and send it over
	// todo
	int32_t fd = open((config["db_path"] + "profile_output.json").c_str(), O_RDONLY);
	int32_t len = lseek(fd, 0, SEEK_END);
	void *buffer = mmap(0, len, PROT_READ, MAP_SHARED, fd, 0);

	send(sock, &len, sizeof(len), 0);
	send(sock, buffer, len, 0);
	std::cout << "Sent file\n";
}

} // namespace duckdb
