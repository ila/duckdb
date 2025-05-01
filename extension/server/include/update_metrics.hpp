//
// Created by ila on 4/28/25.
//

#ifndef UPDATE_METRICS_HPP
#define UPDATE_METRICS_HPP

#include "duckdb.hpp"

namespace duckdb {
string UpdateResponsiveness(string &view_name);
string UpdateCompleteness(int current_window, int ttl_windows, string &view_name);
string UpdateBufferSize(string &view_name);
string CleanupExpiredClients(std::unordered_map<string, string> &config);
} // namespace duckdb

#endif // UPDATE_METRICS_HPP
