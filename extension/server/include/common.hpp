#ifndef COMMON_HPP
#define COMMON_HPP

#include "duckdb.hpp"

namespace duckdb {

// client information legend
#include <string>

enum client_messages { close_connection = 0, new_client = 1, new_result = 2, new_statistics = 3, new_file = 4 };

inline string toString(client_messages msg) {
  switch (msg) {
  case close_connection: return "Close connection";
  case new_client: return "New client";
  case new_result: return "New result";
  case new_statistics: return "New statistics";
  case new_file: return "New file";
  default: return "Unknown message";
  }
}

unordered_map<string, string> ParseConfig(string &path, string &config_name);
void CreateSystemTables(string &path, Connection &con);
void SendFile(std::unordered_map<string, string> &config, int32_t sock);

} // namespace duckdb

#endif
