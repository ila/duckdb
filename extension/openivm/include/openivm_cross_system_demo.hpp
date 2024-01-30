//
// Created by ila on 2-1-24.
//

#ifndef DUCKDB_OPENIVM_CROSS_SYSTEM_DEMO_HPP
#define DUCKDB_OPENIVM_CROSS_SYSTEM_DEMO_HPP

#include "duckdb.hpp"
#include <chrono>
#include <iostream>

namespace duckdb {

void ReplaceTableName(string& query);
void RunIVMCrossSystemDemo(string& catalog, string& schema, string& path);

} // namespace duckdb


#endif // DUCKDB_OPENIVM_CROSS_SYSTEM_DEMO_HPP
