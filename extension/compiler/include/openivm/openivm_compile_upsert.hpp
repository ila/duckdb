#include "duckdb.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/execution/index/art/art.hpp"

#ifndef DUCKDB_COMPILE_UPSERT_H
#define DUCKDB_COMPILE_UPSERT_H

namespace duckdb {
string CompileAggregateGroups(string &view_name, optional_ptr<CatalogEntry> index_delta_view_catalog_entry,
                              vector<string> column_names);
string CompileSimpleAggregates(string &view_name, const vector<string> &column_names);
string CompileProjectionsFilters(string &view_name, const vector<string> &column_names);

} // namespace duckdb

#endif // DUCKDB_COMPILE_UPSERT_H
