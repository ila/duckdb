#include "duckdb.hpp"
#include "rdda_helpers.hpp"

#ifndef DUCKDB_RDDA_TABLE_H
#define DUCKDB_RDDA_TABLE_H

namespace duckdb {
TableScope ParseScope(std::string &query);
string ExtractTableName(const std::string &sql);
vector<RDDAConstraint> ParseCreateTable(string &query);
} // namespace duckdb

#endif // DUCKDB_RDDA_TABLE_H
