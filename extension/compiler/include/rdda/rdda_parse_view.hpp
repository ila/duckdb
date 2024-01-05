#include "duckdb.hpp"
#include "rdda_helpers.hpp"

#ifndef DUCKDB_RDDA_VIEW_H
#define DUCKDB_RDDA_VIEW_H

namespace duckdb {
RDDAViewConstraint ParseCreateView(string &query);
string ParseViewTables(string &query);

} // namespace duckdb

#endif // DUCKDB_RDDA_VIEW_H
