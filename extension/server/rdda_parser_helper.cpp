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

	// first we do some pre-validation
	bool sensitive_found = false;
	bool minimum_aggregation_found = false;
	for (auto constraint : constraints) {
		if (constraint.second.sensitive) {
			sensitive_found = true;
		}
		if (constraint.second.minimum_aggregation > 0) {
			minimum_aggregation_found = true;
		}
		if (sensitive_found && minimum_aggregation_found) {
			break;
		}
	}

	if (!sensitive_found && minimum_aggregation_found) {
		throw ParserException("A table with minimum aggregation must have a sensitive attribute!");
	}

	// write a DFS
	std::stack<LogicalOperator*> node_stack;
	node_stack.push(&plan);
	while (!node_stack.empty()) {
		auto node = node_stack.top();
		node_stack.pop();
		if (node->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto projection = (LogicalProjection *)node;
			for (auto &expr : projection->expressions) {
				// todo check here
				// todo traverse the whole plan
				// todo add checks for functions too
			}
		}
		for (auto &child : node->children) {
			node_stack.push(child.get());
		}
	}





}

}