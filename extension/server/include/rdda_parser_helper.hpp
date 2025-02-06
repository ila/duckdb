//
// Created by ila on 2/4/25.
//

#ifndef RDDA_PARSER_HELPER_HPP
#define RDDA_PARSER_HELPER_HPP

#include "duckdb.hpp"
#include "rdda_parser.hpp"

#include <rdda/rdda_helpers.hpp>

namespace duckdb {

void CheckConstraints(LogicalOperator &plan, unordered_map<string, constraints> &constraints);

}


#endif //RDDA_PARSER_HELPER_HPP
