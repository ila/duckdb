//
// Created by sppub on 10/04/2025.
//

#include "include/logical_plan_to_sql.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_set.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"


namespace duckdb {

std::string CteNode::ToCteQuery() {
	return cte_name + " AS (" + this->ToQuery() + ")";
}

unique_ptr<CteNode> LogicalPlanToSql::CreateCteNode(unique_ptr<LogicalOperator> &subplan, const vector<size_t>& children_indices) {
	const size_t my_index = node_count++;

	switch (subplan->type) {
		case LogicalOperatorType::LOGICAL_GET: {
		    // We need: catalog, schema, table name.
		    unique_ptr<LogicalGet> get_node = unique_ptr_cast<LogicalOperator, LogicalGet>(std::move(subplan));
		    string table_name = get_node->GetTable().get()->name;
		    string catalog_name = ""; // todo
		    string schema_name = get_node->GetTable()->schema.name;
		    vector<string> column_names;
		    vector<string> filters;
		    for (auto &c : get_node->names) {
		        column_names.push_back(c);
		    }
		    // todo - test this logic
			if (!get_node->table_filters.filters.empty()) {
				for (auto &filter : get_node->table_filters.filters) {
					filters.push_back(filter.second->ToString(get_node->names[filter.first]));
				}
			}
		    unique_ptr<GetNode> get_operator = make_uniq<GetNode>(
		    	my_index,
		    	std::move(catalog_name),
		    	std::move(schema_name),
		    	std::move(table_name),
		    	get_node->table_index,
		    	std::move(filters),
		    	std::move(column_names)
		    );
		    return get_operator;
		}
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			unique_ptr<LogicalProjection> projection_node = unique_ptr_cast<LogicalOperator, LogicalProjection>(std::move(subplan));
			vector<string> column_names;
		    for (auto &col : projection_node->expressions) {
				column_names.emplace_back(col->GetName());
			}
			auto project_operator = make_uniq<ProjectNode>(
				my_index, std::move(column_names), projection_node->table_index
			);
			return project_operator;
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		    unique_ptr<LogicalAggregate> aggregate_node = unique_ptr_cast<LogicalOperator, LogicalAggregate>(std::move(subplan));
		    vector<string> aggregate_names;
		    vector<string> group_names;
		    // This logic will break with aliases
		    for (auto &col : aggregate_node->groups) {
		        group_names.emplace_back(col->GetName());
		    }
		    for (auto &exp : aggregate_node->expressions) {
		        aggregate_names.emplace_back(exp->GetName());
		    }
		    auto aggregate_operator = make_uniq<AggregateNode>(
		    	my_index, std::move(aggregate_names), std::move(group_names)
			);
		    return aggregate_operator;
		}
		case LogicalOperatorType::LOGICAL_FILTER: {
		    unique_ptr<LogicalFilter> filter_node = unique_ptr_cast<LogicalOperator, LogicalFilter>(std::move(subplan));
		    vector<string> conditions;
			auto params = filter_node->ParamsToString();
		    // Note: this is always one string regardless of how many expressions are in the filter.
		    // Will it break cross-system compatibility?
			for (auto &c : params) {
				if (c.first == "Expressions") {
				    conditions.emplace_back(c.second.c_str());
			    }
			}
		    auto filter_operator = make_uniq<FilterNode>(my_index, std::move(conditions));
		    return filter_operator;
		}
		case LogicalOperatorType::LOGICAL_UNION: {
		    unique_ptr<LogicalSetOperation> union_node = unique_ptr_cast<LogicalOperator, LogicalSetOperation>(std::move(subplan));
			unique_ptr<UnionNode> union_operator = make_uniq<UnionNode>(
				my_index,
				cte_nodes[children_indices[0]]->cte_name,
				cte_nodes[children_indices[1]]->cte_name,
				union_node->setop_all
			);
			return union_operator;
		}
		// case LogicalOperatorType::LOGICAL_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		    unique_ptr<LogicalComparisonJoin> join_node = unique_ptr_cast<LogicalOperator, LogicalComparisonJoin>(std::move(subplan));
		    vector<string> join_conditions;
		    for (auto &cond : join_node->conditions) {
			    auto left = cond.left->GetName();
			    auto right = cond.right->GetName();
			    auto comparison = ExpressionTypeToOperator(cond.comparison);
			    join_conditions.emplace_back(left + " " + comparison + " " + right);
			}
			unique_ptr<JoinNode> join_operator = make_uniq<JoinNode>(
				my_index,
				cte_nodes[children_indices[0]]->cte_name,
				cte_nodes[children_indices[1]]->cte_name,
				"inner",
				std::move(join_conditions)
			);
			return join_operator;
		}
		default: {
			throw std::runtime_error("This logical operator is not implemented.");
		}
	}
};

unique_ptr<CteNode> LogicalPlanToSql::RecursiveTraversal(unique_ptr<LogicalOperator> &sub_plan) {
	// First run the recursive calls.
	vector<size_t> children_indices;
	for (auto& child: sub_plan-> children) {
		// Get the child as node.
		unique_ptr<CteNode> child_as_node = RecursiveTraversal(child);
		// Store the index, so that it can be used later.
		// `idx` should always match the index inside the vector after insertion.
		// Especially handy if there are multiple children.
		children_indices.push_back(child_as_node->idx);
		cte_nodes.emplace_back(std::move(child_as_node));
	}
	unique_ptr<CteNode> to_return = CreateCteNode(sub_plan, children_indices);
	return to_return;
}

unique_ptr<IRStruct> LogicalPlanToSql::LogicalPlanToIR() {
	// Ensure that this function is not called more than once, to avoid a weird state.
	if (node_count != 0) {
		throw std::runtime_error("This function can only be called once.");
	}
	// Call the recursive traversal to go through the entire plan.
	// Handle the final node here, so that the other nodes can all use CTEs.
	// Same structure as in RecursiveTraversal.
	vector<size_t> children_indices;
	for (auto& child: plan-> children) {
		unique_ptr<CteNode> child_as_node = RecursiveTraversal(child);
		children_indices.push_back(child_as_node->idx);
		cte_nodes.emplace_back(std::move(child_as_node));
	}
	// For the final node, a special case distinction is used.
	// This is because it can have everything a CTE can have, plus INSERT/DELETE/UPDATE.
	// Create the final node (depending on type)...
	unique_ptr<IRStruct> to_return;
	switch (plan->type) {
		case LogicalOperatorType::LOGICAL_INSERT: {
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
			unique_ptr<CteNode> final_node = CreateCteNode(plan, children_indices);
		    to_return = make_uniq<IRStruct>(std::move(cte_nodes), std::move(final_node));
		}
	}
	return to_return;
}

} // namespace duckdb
