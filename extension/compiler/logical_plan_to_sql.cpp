//
// Created by sppub on 10/04/2025.
//

#include "include/logical_plan_to_sql.hpp"


namespace duckdb {

CteNode LogicalPlanToSql::CreateCteNode(unique_ptr<LogicalOperator> &subplan, const vector<size_t>& children_indices) {
	const size_t my_index = node_count++;

	switch (subplan->type) {
		case LogicalOperatorType::LOGICAL_GET: {
		    // We need: catalog, schema, table name.
		}
		case LogicalOperatorType::LOGICAL_PROJECTION:
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		case LogicalOperatorType::LOGICAL_FILTER:
		case LogicalOperatorType::LOGICAL_UNION:
		// case LogicalOperatorType::LOGICAL_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		default: {
			throw std::runtime_error("This logical operator is not implemented.");
		}
	}
};


IRNode LogicalPlanToSql::RecursiveTraversal(unique_ptr<LogicalOperator> &subplan, const bool is_root) {

	// First run the recursive calls.
	vector<size_t> children_indices;
	for (auto& child: subplan-> children) {
		// Get the child as node.
		IRNode child_as_node = RecursiveTraversal(child, false);
		// Store the index, so that it can be used later.
		// `idx` should always match the index inside the vector after insertion.
		// Especially handy if there are multiple children.
		children_indices.push_back(child_as_node.idx);
		// A child node can never be a root node, so static cast it to a CteNode before inserting.
		CteNode& child_as_cte = static_cast<CteNode&>(child_as_node);
		cte_nodes.push_back(std::move(child_as_cte));
	}
	// Handle stuff separately for the final node.
	if (is_root) {
		// Create the final node (depending on type)...
		switch (subplan->type) {
			case LogicalOperatorType::LOGICAL_INSERT: {
			    // Handle this separately.
			    // FIXME: should be implemented!
			    throw std::runtime_error("Not yet implemented.");
			}
			case LogicalOperatorType::LOGICAL_DELETE: {
			    throw std::runtime_error("Not yet implemented.");
			}
			case LogicalOperatorType::LOGICAL_UPDATE: {
			    throw std::runtime_error("Not yet implemented.");
			}
			default: {
			    // Handle it the same way as a CTE.
			    IRNode to_return = CreateCteNode(subplan, children_indices);
			    return to_return;
			}
		}
		throw std::runtime_error("This code should not be reached");
	}
	IRNode to_return = CreateCteNode(subplan, children_indices);
	return to_return;
}


void LogicalPlanToSql::LogicalPlanToIR() {
	// Ensure that this function is not called more than once, to avoid a weird state.
	if (node_count == 0) {
		throw std::runtime_error("This function can only be called once.");
	}
	// Call the recursive traversal to go through the entire plan.
	IRNode final_node = RecursiveTraversal(plan, true);
	cte_vec = CteVec{cte_nodes, final_node};
}

} // namespace duckdb
