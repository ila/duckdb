#include "include/logical_plan_to_string.hpp"
// #include "../postgres_scanner/include/postgres_scanner.hpp"
#include <duckdb/planner/operator/logical_any_join.hpp>

namespace duckdb {

string LogicalPlanToString(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		throw NotImplementedException("Cannot print logical plan with debug_print_bindings enabled");
	}
#endif
	// this function is just to initialize the auxiliary data structures
	// "table index . column index" -> column name
	string plan_string;
	// new name -> old name
	// we need a vector here to preserve the original ordering of columns
	// example: select "a, b, c" should not become select "b, a, c"
	// using trees or hash tables would not preserve the order

	std::unordered_map<string, string> column_names;
	std::vector<std::pair<string, string>> column_aliases;

	string insert_table_name;
	auto prj = unique_ptr<DuckAST>(new DuckAST());
	// now we can call the recursive function
	LogicalPlanToString(context, plan, plan_string, prj, column_names, column_aliases);
	// DuckAST::printAST(prj->root);

	// Printer::Print("Display!-------------");
	prj->generateString(plan_string);
	// Printer::Print(plan_string);
	return plan_string;
}

// TODO: support multiple children (not sure if here or elsewhere).
void LogicalPlanToString(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                         string &plan_string, unique_ptr<DuckAST> &ql_tree,
                         std::unordered_map<string, string> column_names,
                         std::vector<std::pair<string, string>> column_aliases, bool is_second_child) {

	// todo refactor the AST (unnecessary fields) + fix aggregations

	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		auto node = dynamic_cast<LogicalUnconditionalJoin *>(plan.get());
		auto parent_node = ql_tree->getLastNode();
		auto ql_cross_prod = new DuckASTJoin();
		auto node_id = node->GetName() + "_AST";
		auto child1 = dynamic_cast<LogicalGet *>(plan->children[0].get());
		auto child2 = dynamic_cast<LogicalGet *>(plan->children[1].get());
		ql_cross_prod->add_table(child1->GetTable()->name);
		ql_cross_prod->add_table(child2->GetTable()->name);
		auto ql_node = (shared_ptr<DuckASTBaseOperator>)ql_cross_prod;
		ql_tree->insert(ql_node, parent_node, node_id, DuckASTOperatorType::CROSS_JOIN);
		LogicalPlanToString(context, plan->children[0], plan_string, ql_tree, column_names, column_aliases);
		return LogicalPlanToString(context, plan->children[1], plan_string, ql_tree, column_names, column_aliases, true);
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto node = dynamic_cast<LogicalProjection *>(plan.get());
		auto ql_proj_exp = new DuckASTProjection();
		shared_ptr<DuckASTNode> curNode = ql_tree->getLastNode();
		auto bindings = node->GetColumnBindings();
		for (auto &expression : node->expressions) {
			if (expression->type == ExpressionType::BOUND_COLUMN_REF) {
				auto column = dynamic_cast<BoundColumnRefExpression *>(expression.get());
				auto column_index = to_string(column->binding.column_index);
				auto table_index = to_string(column->binding.table_index);
				auto column_name = column->alias;
				ql_proj_exp->add_column(table_index, column_index, column_name);
			}
		}
		auto opr = (shared_ptr<DuckASTBaseOperator>)(ql_proj_exp);
		auto node_id = ql_proj_exp->name + "_AST";
		ql_tree->insert(opr, curNode, node_id, DuckASTOperatorType::PROJECTION);

		if (column_aliases.empty()) {
			// this is the first projection (at least in this first rudimentary implementation)
			for (auto &expression : node->expressions) {
				// todo handle the cases of other expression types
				if (expression->type == ExpressionType::BOUND_COLUMN_REF) {
					auto column = dynamic_cast<BoundColumnRefExpression *>(expression.get());
					auto column_index = column->binding.column_index;
					auto table_index = column->binding.table_index;
					auto column_name = column->alias;
					column_names[std::to_string(table_index) + "." + std::to_string(column_index)] = column_name;
					// we use a placeholder to figure out which column names have been aliased
					column_aliases.emplace_back(column_name, "duckdb_placeholder_internal");
				}
			}
		}

		return LogicalPlanToString(context, plan->children[0], plan_string, ql_tree, column_names, column_aliases);
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto node = dynamic_cast<LogicalFilter *>(plan.get());
		// Extracting the condition in the filter
		auto condition = node->ToString();
		auto ql_filter_exp = new DuckASTFilter(condition);
		shared_ptr<DuckASTNode> curNode = ql_tree->getLastNode();
		auto opr = shared_ptr<DuckASTBaseOperator>(ql_filter_exp);
		auto node_id = node->GetName() + "_AST";
		ql_tree->insert(opr, curNode, node_id, DuckASTOperatorType::FILTER);
		// We just append the filter operator to the ql tree
		return LogicalPlanToString(context, plan->children[0], plan_string, ql_tree, column_names, column_aliases);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto node = dynamic_cast<LogicalAggregate *>(plan.get());
		// Gets column bindings to extract the columns in the group by statement
		// i.e. select sum(x), sum(y) from table group by x, y; we'll get x and y in the node->groups
		// and sum(x) and sum(y) in the node->expressions
		auto binds = node->GetColumnBindings();
		shared_ptr<DuckASTNode> curNode = ql_tree->getLastNode();

		vector<string> group_names;

		for (auto &grp : node->groups) {
			group_names.push_back(grp->GetName());
		}
		vector<string> aggregate_function;
		for (auto &grp : node->expressions) {
			aggregate_function.push_back(grp->GetName());
		}

		std::vector<std::pair<string, string>> aggregate_aliases;
		// this is probably unnecessary but helps code readability
		// we want all the old bindings to be in the same place
		// we iterate groups first, then expressions
		for (auto &group : node->groups) {
			auto column = dynamic_cast<BoundColumnRefExpression *>(group.get());
			aggregate_aliases.emplace_back(std::to_string(column->binding.table_index) + "." +
			                                   std::to_string(column->binding.column_index),
			                               column->alias);
		}
		// "sum(my_col)"
		// my_col

		for (size_t i = 0; i < node->expressions.size(); i++) {
			// todo - rewrite this such that 1) all aggregates are supported, 2) the aggregate function is
			// saved in the AST
			if (node->expressions[i]->type == ExpressionType::BOUND_AGGREGATE) { // should always be true
				auto bound_aggregate = dynamic_cast<BoundAggregateExpression *>(node->expressions[i].get());
				if (!bound_aggregate->children.empty()) {
					auto name = bound_aggregate->function.name;
					auto column = dynamic_cast<BoundColumnRefExpression *>(bound_aggregate->children[0].get());
					if (name == "sum") {
						aggregate_aliases.emplace_back(std::to_string(column->binding.table_index) + "." +
						                                   std::to_string(column->binding.column_index),
						                               "sum(" + column->alias + ")");
					} else if (name == "count") {
						aggregate_aliases.emplace_back(std::to_string(column->binding.table_index) + "." +
						                                   std::to_string(column->binding.column_index),
						                               "count(" + column->alias + ")");
					} else {
						throw NotImplementedException("We only support SUM and COUNT!");
					}
				} else {
					// we are in the count_star() case
					// this does not get bindings - the columns in the scan might be less
					// put a temporary placeholder
					// todo test this
					aggregate_aliases.emplace_back("-1.-1", "count(*)");
				}
			}
		}

		// now we should replace old bindings with new ones in the aggregate aliases
		for (idx_t i = 0; i < binds.size(); i++) {
			auto key = std::to_string(binds[i].table_index) + "." + std::to_string(binds[i].column_index);
			aggregate_aliases[i] = std::make_pair(key, aggregate_aliases[i].second);
		}

		// now we iterate bindings to see if any alias has been replaced
		for (auto &pair : aggregate_aliases) {
			auto it = column_names.find(pair.first);
			if (it != column_names.end()) {
				for (auto &alias_pair : column_aliases) {
					if (alias_pair.first == it->second) {
						alias_pair.second = pair.second;
						break;
					}
				}
			} else {
				throw InternalException("Could not find the column in the column names map!");
			}
		}
		auto node_id = node->GetName() + "_AST";
		auto ql_aggregate_node = new DuckASTAggregate(aggregate_function, group_names);
		auto opr = (shared_ptr<DuckASTBaseOperator>)(ql_aggregate_node);
		opr->name = node_id;
		ql_tree->insert(opr, curNode, node_id, DuckASTOperatorType::AGGREGATE);
		return LogicalPlanToString(context, plan->children[0], plan_string, ql_tree, column_names, column_aliases);
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto node = dynamic_cast<LogicalOrder *>(plan.get());
		shared_ptr<DuckASTNode> curNode = ql_tree->getLastNode();
		auto ql_order_by = new DuckASTOrderBy();
		for (auto &order : node->orders) {
			auto name = order.expression->GetName();
			// Extracts the columns to ORDER BY and the ordering i.e. desc, asc etc
			string order_type;
			switch (order.type) {
			case OrderType::DESCENDING: {
				order_type = "DESC";
				break;
			}
			case OrderType::ASCENDING: {
				order_type = "ASC";
				break;
			}
			default: {
				throw NotImplementedException("We only support ASC and DESC!");
			}
			}
			ql_order_by->add_order_column(name, order_type);
			auto opr = (shared_ptr<DuckASTBaseOperator>)(ql_order_by);
			auto node_id = node->GetName() + "_AST";
			opr->name = node_id;
			ql_tree->insert(opr, curNode, node_id, DuckASTOperatorType::ORDER_BY);
			return LogicalPlanToString(context, plan->children[0], plan_string, ql_tree, column_names, column_aliases);
		}
	}
	case LogicalOperatorType::LOGICAL_GET: {
		// we reached a root node (scan)
		auto node = dynamic_cast<LogicalGet *>(plan.get());

		shared_ptr<DuckASTNode> curNode = ql_tree->getLastNode();
		if (is_second_child) {
			curNode = curNode->parent_node;
		}
		auto ql_get_exp = new DuckASTGet();
		ql_get_exp->name = node->GetName();
		if (node->GetTable().get()) { // DuckDB table scan
			ql_get_exp->table_name = node->GetTable().get()->name;
		} else { // edge case where our base table is a postgres table (postgres scan)
			// todo bug here where sometimes these settings are empty
			// todo make this more generic - add schema and catalog
			Value catalog_value;

			context.TryGetCurrentSetting("ivm_catalog_name", catalog_value);
			Value schema_value;
			context.TryGetCurrentSetting("ivm_schema_name", schema_value);

			string catalog_schema;
			if (!catalog_value.IsNull() && !schema_value.IsNull()) {
				catalog_schema = catalog_value.ToString() + "." + schema_value.ToString() + ".";
			}
			// fixme
			// ql_get_exp->table_name = catalog_schema + dynamic_cast<PostgresBindData *>(node->bind_data.get())->table_name;
		}
		ql_get_exp->all_columns = true;

		if (!node->table_filters.filters.empty()) {
			ql_get_exp->filter_condition = node->ToString().substr(0, node->ToString().find('\n'));
		}

		auto bindings = node->GetColumnBindings();
		auto column_ids = node->GetColumnIds();
		vector<string> scan_column_names;
		if (node->GetTable().get()) {
			scan_column_names = node->GetTable()->GetColumns().GetColumnNames();
		} else {
			// fixme
			// scan_column_names = dynamic_cast<PostgresBindData *>(node->bind_data.get())->names;
		}
		/* 2024-11-03 Not used.
		auto current_table_index = node->GetTableIndex()[0];
        */
		unordered_map<string, string> cur_col_map; // To avoid any changes in ordering of columns

		for (size_t i = 0; i < bindings.size(); i++) {
			auto cur_binding = bindings[i];
			// fixme
			// cur_col_map[to_string(cur_binding.table_index) + "." + to_string(cur_binding.column_index)] =
			//     scan_column_names[column_ids[i]];
		}

		// Checking for aliases with context to the current table only

		// now we check the aliases
		/*
		vector<pair<string, string>> cur_col_aliases;

		for (int i = 0; i < bindings.size(); i++) {
			if (bindings[i].table_index != current_table_index) {
				continue;
			}
			auto key = std::to_string(bindings[i].table_index) + "." + std::to_string(bindings[i].column_index);
			auto it1 = column_names.find(key);
			auto it2 = cur_col_map.find(key);
			if (it2 != cur_col_map.end() && it1 != column_names.end()) {
				if (it1->second == it2->second) {
					cur_col_aliases.push_back({it2->second, "duckdb_placeholder_internal"});
				} else {
					cur_col_aliases.push_back({it2->second, it1->second});
				}
			}
		} */

		for (size_t i = 0; i < bindings.size(); i++) {
			auto key = std::to_string(bindings[i].table_index) + "." + std::to_string(bindings[i].column_index);
			auto it1 = column_names.find(key);
			auto it2 = cur_col_map.find(key);
			if (it1 != cur_col_map.end() && it2 != cur_col_map.end() && it1->second != it2->second) {
				for (auto &pair : column_aliases) {
					if (pair.first == it1->second) {
						pair.second = it2->second;
					}
				}
			}
		}

		if (column_aliases.size() == scan_column_names.size()) {
			// we might be in a SELECT * case
			// we need to check 1) the order and 2) the aliases
			for (size_t i = 0; i < column_aliases.size(); i++) {
				if (column_aliases[i].first != scan_column_names[i] ||
				    column_aliases[i].second != "duckdb_placeholder_internal") {
					ql_get_exp->all_columns = false;
					break;
				}
			}
		} else {
			ql_get_exp->all_columns = false;
		}

		//ql_get_exp->column_aliases = cur_col_aliases;
		ql_get_exp->column_aliases = column_aliases;

		auto opr = (shared_ptr<DuckASTBaseOperator>)ql_get_exp;
		ql_tree->insert(opr, curNode, ql_get_exp->name + "_AST", DuckASTOperatorType::GET);
		break;
	}
	case LogicalOperatorType::LOGICAL_INSERT: {
		// we need to handle this case to support IVM properly
		// the plan to be turned into string starts with an INSERT

		auto node = dynamic_cast<LogicalInsert *>(plan.get());
		auto ql_ins_exp = new DuckASTInsert();
		ql_ins_exp->table_name = node->table.name;
		ql_ins_exp->name = "INSERT_AST"; // the name of the node is private
		shared_ptr<DuckASTNode> curNode = ql_tree->getLastNode();

		auto opr = (shared_ptr<DuckASTBaseOperator>)(ql_ins_exp);
		auto node_id = "INSERT_AST"; // todo remove the id
		ql_tree->insert(opr, curNode, node_id, DuckASTOperatorType::INSERT);
		return LogicalPlanToString(context, plan->children[0], plan_string, ql_tree, column_names, column_aliases);
	}
	default: {
		throw NotImplementedException("We do not support this operator type yet: " + plan->GetName());
	}
	}
}

} // namespace duckdb
