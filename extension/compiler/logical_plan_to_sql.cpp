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
	// Format: "cte_name (col_a, col_b, col_c...) as (select ...)";
	return cte_name + " (" + vec_to_separated_list(cte_column_list)  + ") as (" + this->ToQuery() + ")";
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
			if (use_newlines)
				sql_str << "\n";
		}
	}
	// Then add the final query.
	sql_str << final_node->ToQuery();
	// Finally, add a semicolon.
	sql_str << ";";
	return sql_str.str();
}
std::string LogicalPlanToSql::ColStruct::ToUniqueColumnName() const {
	return "t" + std::to_string(table_index) + "_" + (alias.empty() ? column_name : alias);
}

unique_ptr<CteNode> LogicalPlanToSql::CreateCteNode(unique_ptr<LogicalOperator> &subplan, const vector<size_t>& children_indices) {
	const size_t my_index = node_count++;

	switch (subplan->type) {
		case LogicalOperatorType::LOGICAL_GET: {
			// Preliminary data.
		    //unique_ptr<LogicalGet> plan_as_get = unique_ptr_cast<LogicalOperator, LogicalGet>(std::move(subplan));
			LogicalGet& plan_as_get = static_cast<LogicalGet&>(*subplan);
			auto catalog_entry = plan_as_get.GetTable();
			size_t table_index = plan_as_get.table_index;
		    // We need: catalog, schema, table name.
		    string table_name = catalog_entry.get()->name;
		    string catalog_name = ""; // todo
		    string schema_name = catalog_entry->schema.name;
		    vector<string> column_names;
		    vector<string> filters;

			vector<std::string> cte_column_names;
			// FIXME: col_ids (GetPrimaryIndex()) go 1, 2, 2, 3. So maybe debug to see what's wrong there.
			// auto col_ids = plan_as_get.GetColumnIds();
			const vector<ColumnBinding> col_binds = subplan->GetColumnBindings();
			for (size_t i = 0; i < col_binds.size(); ++i) {
				// Get using `i`.
				std::string column_name = plan_as_get.names[i];
				const ColumnBinding& cb = col_binds[i];
				// Populate stuff.
				column_names.push_back(column_name);
				/* Some logic for the ColStruct. Alias is empty here (=""). */
				auto col_struct = make_uniq<ColStruct>(table_index, column_name, "");
				cte_column_names.push_back(col_struct->ToUniqueColumnName());
		    	column_map[cb] = std::move(col_struct);
				/* End of ColStruct logic */
		    }
		    // todo - test this logic
			if (!plan_as_get.table_filters.filters.empty()) {
				for (auto &filter : plan_as_get.table_filters.filters) {
					filters.push_back(filter.second->ToString(plan_as_get.names[filter.first]));
				}
			}
		    return make_uniq<GetNode>(
		    	my_index,
		    	std::move(cte_column_names),
		    	std::move(catalog_name),
		    	std::move(schema_name),
		    	std::move(table_name),
		    	table_index,
		    	std::move(filters),
		    	std::move(column_names)
		    );
		}
		case LogicalOperatorType::LOGICAL_FILTER: {
			// FIXME: Implement Filter for cte renaming scheme!
			const LogicalFilter& plan_as_filter = static_cast<LogicalFilter&>(*subplan);
		    vector<string> conditions;
			auto params = plan_as_filter.ParamsToString();
		    // Note: this is always one string regardless of how many expressions are in the filter.
		    // Will it break cross-system compatibility?
			for (auto &c : params) {
				if (c.first == "Expressions") {
				    conditions.emplace_back(c.second.c_str());
			    }
			}
		    return make_uniq<FilterNode>(
		    	my_index, vector<string>() /* fixme: fill*/, cte_nodes[children_indices[0]]->cte_name, std::move(conditions)
            );
		}
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			//unique_ptr<LogicalProjection> plan_as_projection = unique_ptr_cast<LogicalOperator, LogicalProjection>(std::move(subplan));
			const LogicalProjection& plan_as_projection = static_cast<LogicalProjection&>(*subplan);
			const size_t table_index = plan_as_projection.table_index;
			/* Logical Projection defines its ColumnBindings using (table_index, expressions.size().
			 * Thus, by looping over expressions.size(), we implicitly have the new column bindings!
			 */
			vector<string> column_names;
			vector<string> cte_column_names;
			for (size_t i = 0; i < plan_as_projection.expressions.size(); ++i) {
				// Stuff.
				const unique_ptr<Expression>& expression = plan_as_projection.expressions[i];
				const ColumnBinding new_cb = ColumnBinding(table_index, i); // `i`: see note above.
				if (expression->type == ExpressionType::BOUND_COLUMN_REF) {
		    		// Cast to BCR expression, to then get column binding.
		    		BoundColumnRefExpression& bcr = static_cast<BoundColumnRefExpression &>(*expression);
		    		// Using the CB, get the ColStruct, to then get the column name.
		    		unique_ptr<ColStruct>& descendant_col_struct = column_map.at(bcr.binding);
					column_names.push_back(descendant_col_struct->ToUniqueColumnName());
					// Create a new ColStruct for this column.
					auto new_col_struct = make_uniq<ColStruct>(
						table_index, descendant_col_struct->column_name, descendant_col_struct->alias
                    );
					cte_column_names.push_back(new_col_struct->ToUniqueColumnName());
					column_map[new_cb] = std::move(new_col_struct);
				} else {
					std::string expr_str = expression->GetName();
                    column_names.emplace_back(expr_str);
					// Create a bespoke name for this expression.
					// TODO: A custom alias is introduced here. May not be fully desirable if it reaches output.
					//  Instead of using custom alias, check if this code suffices.
					std::string scalar_alias;
					if (expression->HasAlias()) {
						scalar_alias = expression->GetAlias();
					} else {
                        scalar_alias = "scalar_" + std::to_string(i);
					}
					column_map[new_cb] = make_uniq<ColStruct>(table_index, expr_str, scalar_alias);
				}
			}
			return make_uniq<ProjectNode>(
				my_index, std::move(cte_column_names), cte_nodes[children_indices[0]]->cte_name, std::move(column_names), table_index
			);
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			// Note: aggregate has several table indices!
			const LogicalAggregate& plan_as_aggregate = static_cast<LogicalAggregate&>(*subplan);
		    // unique_ptr<LogicalAggregate> plan_as_aggregate = unique_ptr_cast<LogicalOperator, LogicalAggregate>(std::move(subplan));
			vector<std::string> cte_column_names;
		    vector<std::string> group_names;
			/* For LogicalAggregate, the function GetColumnBindings cannot be used in our use case,
			/*  since the function combines the `groups`, `aggregate`, and `groupings` together.
			 *  Therefore, we need to make our own enumeration starting from index 0.
			 * If the enumeration scheme of logical aggregates changes, this code should be reviewed closely.
			 * One scope for each of group/aggregate, to avoid mixing up variables.
			 */
			{
				idx_t group_table_index = plan_as_aggregate.group_index;
				const auto& agg_groups = plan_as_aggregate.groups;
				for (size_t i = 0; i < agg_groups.size(); ++i) {
					const unique_ptr<Expression> &col_as_expression = agg_groups[i];
					// Groups should be columns. TODO: Verify that this may only be a BoundColumnRefExpression.
					if (col_as_expression->type == ExpressionType::BOUND_COLUMN_REF) {
						BoundColumnRefExpression& bcr = static_cast<BoundColumnRefExpression &>(*col_as_expression);
						unique_ptr<ColStruct>& descendant_col_struct = column_map.at(bcr.binding);
						// Create new ColStruct *and* provide aggregate names.
						group_names.push_back(descendant_col_struct->ToUniqueColumnName());
						auto new_col_struct = make_uniq<ColStruct>(
							group_table_index, descendant_col_struct->column_name, descendant_col_struct->alias
						);
						cte_column_names.push_back(new_col_struct->ToUniqueColumnName());
						// TODO: Figure out what the new column binding is.
						column_map[ColumnBinding(group_table_index, i)] = std::move(new_col_struct);
					} else {
						throw std::runtime_error("Size mismatch between column bindings!");
					}
					group_names.emplace_back();
					group_names.emplace_back(col_as_expression->GetName());
				}
			}
		    vector<string> aggregate_names;
			{
				idx_t aggregate_table_idx = plan_as_aggregate.aggregate_index;
				const auto& agg_groups = plan_as_aggregate.groups;
				// TODO: Finish.
				for (size_t i = 0; i < agg_groups.size(); ++i) {
					const unique_ptr<Expression> &agg_expression = agg_groups[i];
                    aggregate_names.emplace_back(agg_expression->GetName());
				}
			}
		    return make_uniq<AggregateNode>(
		    	my_index,
		    	cte_column_names,
		    	cte_nodes[children_indices[0]]->cte_name,
		    	std::move(group_names),
		    	std::move(aggregate_names)
			);
		}
		case LogicalOperatorType::LOGICAL_UNION: {
		    // unique_ptr<LogicalSetOperation> plan_as_union = unique_ptr_cast<LogicalOperator, LogicalSetOperation>(std::move(subplan));
			LogicalSetOperation& plan_as_union = static_cast<LogicalSetOperation&>(*subplan);
			const size_t table_index = plan_as_union.table_index;
			// Get the column bindings of the left child. For that, we need to re-access the left child.
			// Then, we can use those to create the new ColStructs for the union (as well as the cte column names).
			vector<string> cte_column_names;
			const auto& lhs_bindings = subplan->children[0]->GetColumnBindings();
			const auto& union_bindings = subplan->GetColumnBindings();
			if (lhs_bindings.size() != union_bindings.size()) {
                throw std::runtime_error("Size mismatch between column bindings!");
			}
			for (size_t i = 0; i < lhs_bindings.size(); ++i) {
				// Get the ColStruct of the child.
				const unique_ptr<ColStruct>& lhs_col_struct = column_map.at(lhs_bindings[i]);
				// Convert the ColStruct into a new ColStruct
				unique_ptr<ColStruct> new_col_struct = make_uniq<ColStruct>(table_index, lhs_col_struct->column_name, lhs_col_struct->alias);
				cte_column_names.push_back(new_col_struct->ToUniqueColumnName());
                column_map[union_bindings[i]] = std::move(new_col_struct);
			}
			return make_uniq<UnionNode>(
				my_index,
				std::move(cte_column_names),
				cte_nodes[children_indices[0]]->cte_name,
				cte_nodes[children_indices[1]]->cte_name,
				plan_as_union.setop_all
			);
		}
		// case LogicalOperatorType::LOGICAL_JOIN:
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		    // unique_ptr<LogicalComparisonJoin> plan_as_join = unique_ptr_cast<LogicalOperator, LogicalComparisonJoin>(std::move(subplan));
			LogicalComparisonJoin& plan_as_join = static_cast<LogicalComparisonJoin&>(*subplan);
		    vector<string> join_conditions;
            /*  The join conditions need to be converted into something that uses the CTEs.
             *  For this, the join conditions are cast to BCR, to then obtain a column binding.
             *  With that column binding, the "unique" column name can be retrieved.
             *
             *	This logic may be a bit complicated, but is necessary to avoid issues if both sides of the join
             *	 use the same column names.
             *	Doing it this way also avoids issue with the assumption below.
             *  This assumption may still be relevant of a join condition does not use the type BOUND_COLUMN_REF.
             *
             *  IMPORTANT ASSUMPTION: the lhs of a join condition *always* corresponds to the left child,
             *   and the rhs of a join condition *always* corresponds to the right child.
             *  If this assumption turns out to not hold, this logic has to be revised (e.g. with table index checks).
             */
			std::string left_cte_name = cte_nodes[children_indices[0]]->cte_name;
			std::string right_cte_name = cte_nodes[children_indices[1]]->cte_name;
		    for (auto &cond : plan_as_join.conditions) {
		    	std::string condition_lhs, condition_rhs;
		    	if (cond.left->type == ExpressionType::BOUND_COLUMN_REF) {
		    		// Cast to BCR expression, to then get column binding.
		    		const BoundColumnRefExpression& l_ref = static_cast<BoundColumnRefExpression &>(*cond.left);
		    		// Using the CB, get the ColStruct, to then get the column name.
		    		unique_ptr<ColStruct>& l_col_struct = column_map.at(l_ref.binding);
		    		condition_lhs = l_col_struct->ToUniqueColumnName();
		    	} else {
                    throw NotImplementedException("Join conditions currently only support BCR expressions.");
		    	}
		    	if (cond.right->type == ExpressionType::BOUND_COLUMN_REF) {
		    		// Cast to BCR expression, to then get column binding.
					const BoundColumnRefExpression& r_ref = static_cast<BoundColumnRefExpression &>(*cond.right);
		    		// Using the CB, get the ColStruct, to then get the table name.
		    		unique_ptr<ColStruct>& r_col_struct = column_map.at(r_ref.binding);
		    		condition_rhs = r_col_struct->ToUniqueColumnName();
		    	} else {
                    throw NotImplementedException("Join conditions currently only support BCR expressions.");
		    	}
			    auto comparison = ExpressionTypeToOperator(cond.comparison);
		    	// Put brackets around the join conditions to avoid incorrect queries if there are multiple conditions.
			    join_conditions.emplace_back("(" + condition_lhs + " " + comparison + " " + condition_rhs + ")");
			}
			// Finally, get the column_names.
			vector<std::string> cte_column_names;
			for (const auto& binding : plan_as_join.GetColumnBindings()) {
                unique_ptr<ColStruct>& col_struct = column_map.at(binding);
				cte_column_names.push_back(col_struct->ToUniqueColumnName());
			}
			return make_uniq<JoinNode>(
				my_index,
				std::move(cte_column_names),
				std::move(left_cte_name),
				std::move(right_cte_name),
				plan_as_join.join_type,
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
			// Use a cast ref to the pointer (rather than a unique_ptr_cast) to keep `plan' ownership intact.
			//  This is possible because this code does not modify the plan.
			const LogicalInsert& insert_ref = static_cast<LogicalInsert&>(*plan);
			//auto plan_to_insert_node = unique_ptr_cast<LogicalOperator, LogicalInsert>(std::move(plan));
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
