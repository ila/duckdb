#include "include/common.hpp"

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/connection.hpp"

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
				// strip strings from whitespaces
				key = std::regex_replace(key, std::regex("^ +| +$|( ) +"), "$1");
				value = std::regex_replace(value, std::regex("^ +| +$|( ) +"), "$1");
				config.insert(make_pair(key, value));
			}
		}
	}
	// todo exception handling
	return config;
}

void CreateSystemTables(string &path, Connection &con) {

	// there is currently no way to execute SQL statements in files through the C++ API
	// so here we go again...
	std::ifstream input_file(path + "tables.sql");
	string query;

	while (getline(input_file, query)) {
		std::istringstream config_line(query);
		auto result = con.Query(query);
		if (result->HasError()) {
			throw ParserException("Error while creating system tables: " + result->GetError());
		}
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
}

} // namespace duckdb
