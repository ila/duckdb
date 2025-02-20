#include "rdda_parser_helper.hpp"

#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/planner.hpp>
#include <rdda/rdda_helpers.hpp>
#include <stack>

namespace duckdb {

void CheckConstraints(LogicalOperator &plan, unordered_map<string, constraints> &constraints) {

	if (constraints.empty()) {
		throw ParserException("Decentralized tables must have privacy-preserving constraints!");
	}

	// checks:
	// 1. a sensitive column cannot be in a projection
	// 2. if a sensitive column is in a group by clause, there must be a minimum aggregation (in any other column)
	// 3. if a column has minimum aggregation, there must be another sensitive column in the same table
	// 4. minimum aggregation columns must be used in a query with a group by

	// todo - implement this

}

}