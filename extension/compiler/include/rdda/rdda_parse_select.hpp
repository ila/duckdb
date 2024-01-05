//
// Created by ila on 19-4-23.
//

#include "duckdb.hpp"
#include "rdda_helpers.hpp"

#ifndef DUCKDB_RDDA_SELECT_HPP
#define DUCKDB_RDDA_SELECT_HPP

namespace duckdb {
RDDASelectOption ParseSelectQuery(string &query);
}

#endif // DUCKDB_RDDA_SELECT_HPP
