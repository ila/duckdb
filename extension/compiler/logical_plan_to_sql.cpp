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

namespace {

using duckdb::vector;

/// Convert a vector of strings into a comma-separated list (i.e. "a, b, c, d., ...").
std::string vec_to_separated_list(vector<std::string> input_list, const std::string_view separator = ", ") {
	std::ostringstream ret_str;
	for (size_t i = 0; i < input_list.size(); ++i) {
		ret_str << input_list[i];
		if (i != input_list.size() - 1) {
			ret_str << separator;
		}
	}
	return ret_str.str();
}

} // namespace

namespace duckdb {

std::string InsertNode::ToQuery() {
	// Currently does not support the type with "INSERT ... VALUES.
	std::stringstream insert_str;
	insert_str << "insert ";
	switch (action_type) {
	case OnConflictAction::THROW:
		break; // Nothing needs to be added
	case OnConflictAction::REPLACE:  // Should not even occur.
	case OnConflictAction::UPDATE:
		insert_str << "or replace "; break;
	case OnConflictAction::NOTHING:
		insert_str << "or ignore "; break;
	default:
		throw NotImplementedException("OnConflictAction::%s is not (yet) supported", EnumUtil::ToString(action_type));
	}
	insert_str << "into ";
	insert_str << target_table;
	insert_str << " (select * from ";
	insert_str << child_cte_name;
	insert_str << ")";
	return insert_str.str();
}
std::string CteNode::ToCteQuery() {
	return cte_name + " as (" + this->ToQuery() + ")";
}
std::string GetNode::ToQuery() {
	std::ostringstream get_str;
	get_str << "select ";
	if (column_names.empty()) {
		get_str << "*";
	} else {
		get_str << vec_to_separated_list(column_names);
	}
	get_str << " from ";
	get_str << table_name;
	if (table_filters.empty()) {
		// Add nothing.
	} else {
		get_str << " where ";
		get_str << vec_to_separated_list(table_filters);
	}
	// Note: no semicolon at the end; this is handled by the IR function.
	return get_str.str();
}
std::string FilterNode::ToQuery() {
	std::ostringstream get_str;
	get_str << "select * from ";
	get_str << child_cte_name;
	if (conditions.empty()) {
		// Add nothing.
	} else {
		get_str << " where ";
		get_str << vec_to_separated_list(conditions);
	}
	return get_str.str();
}
std::string ProjectNode::ToQuery() {
	std::ostringstream project_str;
	project_str << "select ";
	if (column_names.empty()) {
		project_str << "*";
	} else {
		project_str << vec_to_separated_list(column_names);
	}
	project_str << " from ";
	project_str << child_cte_name;
	return project_str.str();
}

std::string AggregateNode::ToQuery() {
	std::ostringstream aggregate_str;
	aggregate_str << "select ";
	if (group_names.empty()) {
		// Do nothing.
	} else {
		aggregate_str << vec_to_separated_list(group_names);
		aggregate_str << ", "; // Needed for the aggregate names.
	}
	aggregate_str << vec_to_separated_list(aggregate_names);
	aggregate_str << " from ";
	aggregate_str << child_cte_name;
	if (group_names.empty()) {
		// Do nothing.
	} else {
		aggregate_str << " group by ";
		aggregate_str << vec_to_separated_list(group_names);
	}
	return aggregate_str.str();
}
std::string JoinNode::ToQuery() {
	std::ostringstream join_str;
	join_str << "select * from ";
	join_str << left_cte_name;
	/* Assumption made: the LEFT side of a condition is always related to the *left* CTE,
	 * Whereas the RIGHT side of a condition is always related to the *right* CTE.
	 * If this condition turns out to not be true, the logic here will be significantly harder,
	 *  as for every condition there would need to be a check on which side is which.
	 */
	join_str << " ";
	switch (join_type) {
		case JoinType::INNER:
		case JoinType::LEFT:
		case JoinType::RIGHT:
		case JoinType::OUTER:
			join_str << EnumUtil::ToString(join_type); break;
	default:
		throw NotImplementedException("JoinType::%s is not (yet) supported", EnumUtil::ToString(join_type));
	}
	join_str << " join ";
	join_str << right_cte_name;
	join_str << " on ";
	// Combine the join conditions using AND.
	// In theory, OR should also be possible, but this is rare enough to not require support for now.
	join_str << vec_to_separated_list(join_conditions, " AND ");
	return join_str.str();
}
std::string UnionNode::ToQuery() {
	std::ostringstream union_str;
	union_str << "select * from ";
	union_str << left_cte_name;
	if (is_union_all) {
		union_str << " union all ";
	} else {
		union_str << " union ";
	}
	union_str << "select * from ";
	union_str << right_cte_name;
	return union_str.str();
}
std::string IRStruct::ToQuery(const bool use_newlines) {
	std::ostringstream sql_str;
	// First add all CTEs...
	if (nodes.empty()) {
		// Do nothing.
	} else {
		sql_str << "with ";
		for (size_t i = 0; i < nodes.size(); ++i) {
			sql_str << nodes[i]->ToCteQuery();
			if (i != nodes.size() - 1) {
				sql_str << ", ";
			} else if (!use_newlines) {
				// Add extra space for main query (if newlines disabled).
				sql_str << " ";
			}
			if (use_newlines) sql_str << "\n";
		}
	}
	// Then add the final query.
	sql_str << final_node->ToQuery();
	// Finally, add a semicolon.
	sql_str << ";";
	return sql_str.str();
}

unique_ptr<CteNode> LogicalPlanToSql::CreateCteNode(unique_ptr<LogicalOperator> &subplan, const vector<size_t>& children_indices) {
	const size_t my_index = node_count++;

	switch (subplan->type) {
		case LogicalOperatorType::LOGICAL_GET: {
		    // We need: catalog, schema, table name.
		    unique_ptr<LogicalGet> plan_as_get = unique_ptr_cast<LogicalOperator, LogicalGet>(std::move(subplan));
		    string table_name = plan_as_get->GetTable().get()->name;
		    string catalog_name = ""; // todo
		    string schema_name = plan_as_get->GetTable()->schema.name;
		    vector<string> column_names;
		    vector<string> filters;
		    for (auto &c : plan_as_get->names) {
		        column_names.push_back(c);
		    }
		    // todo - test this logic
			if (!plan_as_get->table_filters.filters.empty()) {
				for (auto &filter : plan_as_get->table_filters.filters) {
					filters.push_back(filter.second->ToString(plan_as_get->names[filter.first]));
				}
			}
		    return make_uniq<GetNode>(
		    	my_index,
		    	std::move(catalog_name),
		    	std::move(schema_name),
		    	std::move(table_name),
		    	plan_as_get->table_index,
		    	std::move(filters),
		    	std::move(column_names)
		    );
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
		    return make_uniq<FilterNode>(my_index, cte_nodes[children_indices[0]]->cte_name, std::move(conditions));
		}
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			unique_ptr<LogicalProjection> plan_as_projection = unique_ptr_cast<LogicalOperator, LogicalProjection>(std::move(subplan));
			vector<string> column_names;
			// FIXME: col->GetName() does not get column name if more than one table index exists.
			//  Instead, it prints the column bindings. Example:
			// `projection_3 as (select #[7.0], #[7.1], #[7.3], #[0.0], #[0.1], #[7.2] from join_2),`
		    for (auto &col : plan_as_projection->expressions) {
				column_names.emplace_back(col->GetName());
			}
			return make_uniq<ProjectNode>(
				my_index, cte_nodes[children_indices[0]]->cte_name, std::move(column_names), plan_as_projection->table_index

			);
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		    unique_ptr<LogicalAggregate> plan_as_aggregate = unique_ptr_cast<LogicalOperator, LogicalAggregate>(std::move(subplan));
		    vector<string> aggregate_names;
		    vector<string> group_names;
		    // Note: this logic will break with aliases.
		    for (auto &col : plan_as_aggregate->groups) {
		        group_names.emplace_back(col->GetName());
		    }
		    for (auto &exp : plan_as_aggregate->expressions) {
		        aggregate_names.emplace_back(exp->GetName());
		    }
		    return make_uniq<AggregateNode>(
		    	my_index, cte_nodes[children_indices[0]]->cte_name, std::move(group_names), std::move(aggregate_names)
			);
		}
		case LogicalOperatorType::LOGICAL_UNION: {
		    unique_ptr<LogicalSetOperation> plan_as_union = unique_ptr_cast<LogicalOperator, LogicalSetOperation>(std::move(subplan));
			return make_uniq<UnionNode>(
				my_index,
				cte_nodes[children_indices[0]]->cte_name,
				cte_nodes[children_indices[1]]->cte_name,
				plan_as_union->setop_all
			);
		}
		// case LogicalOperatorType::LOGICAL_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		    unique_ptr<LogicalComparisonJoin> plan_as_join = unique_ptr_cast<LogicalOperator, LogicalComparisonJoin>(std::move(subplan));
		    vector<string> join_conditions;
            /*  The join conditions need to be converted into something that uses the CTEs.
             *  In other words, the strings for `left` and `right` should contain the CTE names for either side.
             *  Basically, something like: {left_cte}.{left_condition_column} = {right_cte}.{right_condition_column}.
             *  This is to avoid conflicts in case both tables have a column with the same name.
             *
             *  IMPORTANT ASSUMPTION: the lhs of a join condition *always* corresponds to the left child,
             *   and the rhs of a join condition *always* corresponds to the right child.
             *  If this assumption turns out to not hold, this logic has to be revised (e.g. with table index checks).
             */
			std::string left_cte_name = cte_nodes[children_indices[0]]->cte_name;
			std::string right_cte_name = cte_nodes[children_indices[1]]->cte_name;
		    for (auto &cond : plan_as_join->conditions) {
			    std::string condition_lhs = left_cte_name + "." + cond.left->GetName();
			    std::string condition_rhs = right_cte_name + "." + cond.right->GetName();
			    auto comparison = ExpressionTypeToOperator(cond.comparison);
		    	// Put brackets around the join conditions to avoid incorrect queries if there are multiple conditions.
			    join_conditions.emplace_back("(" + condition_lhs + " " + comparison + " " + condition_rhs + ")");
			}
			return make_uniq<JoinNode>(
				my_index,
				std::move(left_cte_name),
				std::move(right_cte_name),
				plan_as_join->join_type,
				std::move(join_conditions)
			);
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
			// FIXME: Modify the unique_ptr_cast also as follows (to avoid ownership issues).
			// Use a reinterpreted ref to the pointer (rather than a unique_ptr_cast) to keep `plan' ownership intact.
			//  This is possible because this code does not modify the plan.
			LogicalInsert& insert_ref = reinterpret_cast<LogicalInsert&>(*plan);
			// auto plan_to_insert_node = unique_ptr_cast<LogicalOperator, LogicalInsert>(std::move(plan));
			unique_ptr<InsertNode> insert_node = make_uniq<InsertNode>(
				node_count++,
				insert_ref.table.name,
				cte_nodes[children_indices[0]]->cte_name,
				insert_ref.action_type
			);
			to_return = make_uniq<IRStruct>(std::move(cte_nodes), std::move(insert_node));
			// Finally, cast back the plan to a LogicalOperator to avoid issues when using the plan later on.
			// plan = unique_ptr_cast<LogicalOperator, LogicalInsert>(std::move(plan_to_insert_node));
			break;
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
