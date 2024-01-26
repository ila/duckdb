//
// Created by ila on 2-10-23.
//

#ifndef DUCKDB_LOGICAL_PLAN_TO_STRING_HPP
#define DUCKDB_LOGICAL_PLAN_TO_STRING_HPP

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"

namespace duckdb {
string LogicalPlanToString(unique_ptr<LogicalOperator> &plan);
void LogicalPlanToString(unique_ptr<LogicalOperator> &plan, string &plan_string,
                         std::unordered_map<string, string> &column_names,
                         std::vector<std::pair<string, string>> &column_aliases, 
                         string &insert_table_name, bool do_join);
} // namespace duckdb

#endif // DUCKDB_LOGICAL_PLAN_TO_STRING_HPP
