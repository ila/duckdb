//
// Created by ila on 2/24/25.
//

#ifndef RUN_SERVER_HPP
#define RUN_SERVER_HPP

#include <duckdb.hpp>

namespace duckdb {
void SignalHandler(int signal);
void SerializeQueryPlan(string &query, string &path, string &dbname);
void ParseJSON(Connection &con, std::unordered_map<string, string> &config, int32_t connfd, hugeint_t client);
void DeserializeQueryPlan(string &path, string &dbname);
void InsertClient(Connection &con, unordered_map<string, string> &config, uint64_t id, const string_t &timestamp);
void RunServer(ClientContext &context, const FunctionParameters &parameters);
} // namespace duckdb

#endif // RUN_SERVER_HPP
