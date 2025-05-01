//
// Created by ila on 2/24/25.
//

#ifndef CLIENT_FUNCTIONS_HPP
#define CLIENT_FUNCTIONS_HPP

#include <duckdb.hpp>

namespace duckdb {
void CloseConnection(int32_t sock);
int32_t ConnectClient(unordered_map<string, string> &config);
void SendChunks(std::unique_ptr<MaterializedQueryResult> &result, int32_t sock);
timestamp_t RefreshMaterializedView(string &view_name, Connection &con);
int32_t SendResults(string &view_name, timestamp_t timestamp, Connection &con, string &path);
} // namespace duckdb

#endif // CLIENT_FUNCTIONS_HPP
