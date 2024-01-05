#ifndef COMMON_HPP
#define COMMON_HPP

#include "duckdb.hpp"

namespace duckdb {

// client information legend
enum client_messages { close_connection = 0, new_client = 1, new_result = 2, new_statistics = 3 }; // int32_t

unordered_map<string, string> ParseConfig(string &path, string &config_name);
void CreateSystemTables(string &path, string &db_path, string &db_name, string &schema_name, Connection &con);
void SendFile(std::unordered_map<string, string> &config, int32_t sock);

} // namespace duckdb

#endif
