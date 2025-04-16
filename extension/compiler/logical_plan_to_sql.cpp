//
// Created by sppub on 10/04/2025.
//

#include "include/logical_plan_to_sql.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
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
std::string vec_to_separated_list(vector<std::string> input_list, const std::string& separator = ", ") {
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
	// Note: no parentheses needed here!
	insert_str << " select * from ";
	insert_str << child_cte_name;
	return insert_str.str();
}
std::string CteNode::ToCteQuery() {
	// Format: "cte_name (col_a, col_b, col_c...) as (select ...)";
	std::ostringstream cte_str;
	cte_str << cte_name;
	if (!cte_column_list.empty()) {
		cte_str << " (";
		cte_str << vec_to_separated_list(cte_column_list);
		cte_str << ")";
	}
	cte_str << " as (";
	cte_str << this->ToQuery();
	cte_str << ")";
	return cte_str.str();
	// 	return cte_name + " (" + vec_to_separated_list(cte_column_list)  + ") as (" + this->ToQuery() + ")";
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
	if (group_by_columns.empty()) {
		// Do nothing.
	} else {
		aggregate_str << vec_to_separated_list(group_by_columns);
		aggregate_str << ", "; // Needed for the aggregate names.
	}
	aggregate_str << vec_to_separated_list(aggregate_expressions);
	aggregate_str << " from ";
	aggregate_str << child_cte_name;
	if (group_by_columns.empty()) {
		// Do nothing.
	} else {
		aggregate_str << " group by ";
		aggregate_str << vec_to_separated_list(group_by_columns);
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
			if (use_newlines) {
				sql_str << "\n";
			}
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

std::string LogicalPlanToSql::FilterToString(const unique_ptr<Expression>& expression) const {
	// Maybe needed for the specifics inside an operator.
	const ExpressionClass e_class = expression->GetExpressionClass();
	std::ostringstream expr_str;
	switch (e_class) {
	case (ExpressionClass::BOUND_COLUMN_REF): {
		// Get the column ref and print the corresponding `ColStruct`'s unique column name.
		const BoundColumnRefExpression& bcr = expression->Cast<BoundColumnRefExpression>();
		const unique_ptr<ColStruct>& col_struct = column_map.at(bcr.binding);
		expr_str << col_struct->ToUniqueColumnName();
		break;
	}
	case (ExpressionClass::BOUND_CONSTANT): {
		expr_str << expression->ToString(); // Constants can remain as-is.
		break;
	}
	// Recursive calls.
	case (ExpressionClass::BOUND_COMPARISON): {
		// Attempt to cast to a BoundComparisonExpression.
		const BoundComparisonExpression& expr_cast = expression->Cast<BoundComparisonExpression>();
		/* A BoundComparisonExpression has a left and right side.
			 * If those sides are BoundColumnRefExpressions, they can be converted to CTE table names.
			 * Otherwise, the operators will be used as-is (i.e. with the default ToString() function).
			 * In any case, make this a recursive call.
		 */
		expr_str << "(";
		expr_str << FilterToString(expr_cast.left);
		expr_str << ") ";
		expr_str << ExpressionTypeToOperator(expr_cast.GetExpressionType());
		expr_str << " (";
		expr_str << FilterToString(expr_cast.right);
		expr_str << ")";
		break;
	}
	case (ExpressionClass::BOUND_CAST): {
		const BoundCastExpression& expr_cast = expression->Cast<BoundCastExpression>();
		// Based on BoundCastExpression::ToString().
		expr_str << (expr_cast.try_cast ? "TRY_CAST(" : "CAST(");
		expr_str << FilterToString(expr_cast.child);
		expr_str <<  " AS " + expr_cast.return_type.ToString() + ")";
		break;
	}
	case (ExpressionClass::BOUND_CONJUNCTION): {
		// Attempt to cast to a BoundConjunctionExpression.
		const BoundConjunctionExpression& conjunction_expr = expression->Cast<BoundConjunctionExpression>();
		// A conjunction can currently be one of: AND, OR.
		// Further, a conjunction has a left and right side, each of which are in the vector of children.
		// In turn, these "children" can be arbitrary expressions, so call recursively to resolve those.
		expr_str << "(";
		expr_str << FilterToString(conjunction_expr.children[0]); // Left child.
		expr_str << ") ";
		expr_str << ExpressionTypeToOperator(conjunction_expr.GetExpressionType());
		expr_str << " (";
		expr_str << FilterToString(conjunction_expr.children[1]); // Right child.
		expr_str << ")";
		break;
	} default: {
		throw NotImplementedException("Unsupported expression for logical filter.");
	}
	}
	return expr_str.str();
}

unique_ptr<CteNode> LogicalPlanToSql::CreateCteNode(unique_ptr<LogicalOperator> &subplan, const vector<size_t>& children_indices) {
	const size_t my_index = node_count++;

	switch (subplan->type) {
		case LogicalOperatorType::LOGICAL_GET: {
			// Preliminary data.
		    const LogicalGet& plan_as_get = subplan->Cast<LogicalGet>();
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
			const LogicalFilter& plan_as_filter = subplan->Cast<LogicalFilter>();
		    vector<string> conditions;
		    // A LogicalFilter is constructed with only one expression.
		    // Hence, we can assert that there is only one expression in the expression vector.
		    // However, to be certain, this is not done here.
		    for (const unique_ptr<Expression>& expression: plan_as_filter.expressions) {
				conditions.emplace_back(FilterToString(expression));
		    }
		    return make_uniq<FilterNode>(
		    	my_index, vector<string>() /* fixme: fill*/, cte_nodes[children_indices[0]]->cte_name, std::move(conditions)
            );
		}
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			const LogicalProjection& plan_as_projection = subplan->Cast<LogicalProjection>();
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
		    		BoundColumnRefExpression& bcr = expression->Cast<BoundColumnRefExpression>();
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
			const LogicalAggregate& plan_as_aggregate = subplan->Cast<LogicalAggregate>();
			vector<std::string> cte_column_names;
		    vector<std::string> group_names;
			/* For LogicalAggregate, the function GetColumnBindings cannot be used in our use case,
			 *  since the function combines the `groups`, `aggregate`, and `groupings` together.
			 *  Therefore, we need to make our own enumeration starting from index 0.
			 * If the enumeration scheme of logical aggregates changes, this code should be reviewed closely.
			 * One scope for each of group/aggregate, to avoid mixing up variables.
			 */
			{
				const idx_t group_table_index = plan_as_aggregate.group_index;
				const auto& groups = plan_as_aggregate.groups;
				for (size_t i = 0; i < groups.size(); ++i) {
					const unique_ptr<Expression> &col_as_expression = groups[i];
					// Groups should be columns. TODO: Verify that this may only be a BoundColumnRefExpression.
					if (col_as_expression->type == ExpressionType::BOUND_COLUMN_REF) {
						BoundColumnRefExpression& bcr = col_as_expression->Cast<BoundColumnRefExpression>();
						unique_ptr<ColStruct>& descendant_col_struct = column_map.at(bcr.binding);
						// Create new ColStruct *and* provide aggregate names.
						group_names.emplace_back(descendant_col_struct->ToUniqueColumnName());
						auto new_col_struct = make_uniq<ColStruct>(
							group_table_index, descendant_col_struct->column_name, descendant_col_struct->alias
						);
						cte_column_names.emplace_back(new_col_struct->ToUniqueColumnName());
					    // `i` used, because of the comment above the scope.
						column_map[ColumnBinding(group_table_index, i)] = std::move(new_col_struct);
					} else {throw NotImplementedException("Only supporting BoundColumnRef for now.");}
				}
			}
		    vector<string> aggregate_names;
			{
				const idx_t aggregate_table_idx = plan_as_aggregate.aggregate_index;
				const auto& agg_expressions = plan_as_aggregate.expressions;
			    unique_ptr<ColStruct> agg_col_struct;
				for (size_t i = 0; i < agg_expressions.size(); ++i) {
					const unique_ptr<Expression> &expr = agg_expressions[i];
				    if (expr->type == ExpressionType::BOUND_AGGREGATE) {
						BoundAggregateExpression& bound_agg = expr->Cast<BoundAggregateExpression>();
						std::ostringstream agg_str;
						agg_str << bound_agg.function.name; // Add the function name (example: "sum" or "count").
					    agg_str << "(";
						// Then (inside parentheses) we need to know if it's distinct or not.
					    if (bound_agg.IsDistinct()) {
						    agg_str << "distinct ";
					    }
						// To complete `agg_str`, we need to know what columns are in the aggregate type.
						// These are "child"-expressions to the aggregate expression.
						vector<std::string> child_expressions;
						for (const unique_ptr<Expression>& agg_child : bound_agg.children) {
							if (agg_child->type == ExpressionType::BOUND_COLUMN_REF) {
								BoundColumnRefExpression& bcr = agg_child->Cast<BoundColumnRefExpression>();
								const unique_ptr<ColStruct>& child_col_struct = column_map.at(bcr.binding);
								child_expressions.emplace_back(child_col_struct->ToUniqueColumnName());
							} else {throw NotImplementedException("Only supporting BoundColumnRef for now.");}
						}
					    agg_str << vec_to_separated_list(child_expressions);
						agg_str << ")"; // Don't forget to close the parenthesis!
					    // Give the aggregation an alias so that this string above does not have to be repeated.
					    std::string agg_alias = "aggregate_" + std::to_string(i);
					    // Finally, Put it all into a ColStruct (which also handles the column vectors below).
					    agg_col_struct = make_uniq<ColStruct>(
					        aggregate_table_idx, agg_str.str(), std::move(agg_alias)
						);
				    } else {
						throw NotImplementedException("Only supporting BoundAggregateExpression for now.");
				    }
				    // `column_name` is the string with the function name (e.g. "count(distinct age)").
				    aggregate_names.emplace_back(agg_col_struct->column_name);
				    cte_column_names.emplace_back(agg_col_struct->ToUniqueColumnName());
					// `i` used, because of the comment above the scopes.
					column_map[ColumnBinding(aggregate_table_idx, i)] = std::move(agg_col_struct);
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
		    // Cannot be const, since GetColumnBindings() is not const.
			LogicalSetOperation& plan_as_union = subplan->Cast<LogicalSetOperation>();
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
			LogicalComparisonJoin& plan_as_join = subplan->Cast<LogicalComparisonJoin>();
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
		    		const BoundColumnRefExpression& l_ref = cond.left->Cast<BoundColumnRefExpression>();
		    		// Using the CB, get the ColStruct, to then get the column name.
		    		unique_ptr<ColStruct>& l_col_struct = column_map.at(l_ref.binding);
		    		condition_lhs = l_col_struct->ToUniqueColumnName();
		    	} else {
                    throw NotImplementedException("Join conditions currently only support BCR expressions.");
		    	}
		    	if (cond.right->type == ExpressionType::BOUND_COLUMN_REF) {
		    		// Cast to BCR expression, to then get column binding.
					const BoundColumnRefExpression& r_ref = cond.right->Cast<BoundColumnRefExpression>();
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
			// Use the non-unique Cast function to keep the `plan` ownership intact.
			//  This is possible because this code does not modify the plan.
			const LogicalInsert& insert_ref = plan->Cast<LogicalInsert>();
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
		    /* TODO: Change this code to properly support aliases! Plan of action:
		     *  - add what is now "final_node" to the cte_nodes.
		     *  - using the `plan`, get the ColumnBindings of the topmost node.
		     *  - for each CB, get its ColStruct from which you extract the alias (or column name if no alias).
		     *  - create a new node that uses *those* aliases in the select statement.
		     */
			// Handle it the same way as a CTE.
			unique_ptr<CteNode> final_node = CreateCteNode(plan, children_indices);
		    to_return = make_uniq<IRStruct>(std::move(cte_nodes), std::move(final_node));
		}
	}
	return to_return;
}


} // namespace duckdb