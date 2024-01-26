//
// Created by ila on 2-1-24.
//

#ifndef DUCKDB_IVM_CROSS_SYSTEM_DEMO_HPP
#define DUCKDB_IVM_CROSS_SYSTEM_DEMO_HPP

#include "duckdb.hpp"
#include <chrono>
#include <iostream>

namespace duckdb {

void ReplaceTableName(string& query);
void RunIVMCrossSystemDemo(string& path);

// Logical Plan to string demo
void RunLogicalPlanToString(string& sql_string);

} // namespace duckdb


#endif // DUCKDB_IVM_CROSS_SYSTEM_DEMO_HPP
