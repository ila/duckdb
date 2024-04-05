#include "include/logical_plan_to_string.hpp"

namespace duckdb {

string LogicalPlanToString(unique_ptr<LogicalOperator> &plan) {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		throw NotImplementedException("Cannot print logical plan with debug_print_bindings enabled");
	}
#endif
	// this function is just to initialize the auxiliary data structures
	// "table index . column index" -> column name
	std::unordered_map<string, string> column_names;
	string plan_string;
	// new name -> old name
	// we need a vector here to preserve the original ordering of columns
	// example: select "a, b, c" should not become select "b, a, c"
	// using trees or hash tables would not preserve the order
	std::vector<std::pair<string, string>> column_aliases;
	string insert_table_name;
	auto prj = unique_ptr<DuckAST>(new DuckAST());
	// now we can call the recursive function
	map<string, string> colm_aliases;
	LogicalPlanToString(plan, plan_string, "", prj, colm_aliases);
	DuckAST::printAST(prj->root);

	Printer::Print("Display!-------------");
	prj->generateString(plan_string);
	Printer::Print(plan_string);
	return plan_string;
}

void LogicalPlanToString(unique_ptr<LogicalOperator> &plan, string &plan_string, string cur_parent,
                         unique_ptr<DuckAST> &ql_tree, map<string, string> &column_map) {
	if (cur_parent.size() == 0) {
		cur_parent = "native";
		//
	}
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto node = dynamic_cast<LogicalProjection *>(plan.get());
		auto ql_proj_exp = new DuckASTProjection();
		ql_proj_exp->name = node->GetName();
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
		auto expr = (shared_ptr<DuckASTBaseOperator>)(ql_proj_exp);
		auto node_id = cur_parent + "_" + ql_proj_exp->name;
		ql_tree->insert(expr, node_id, DuckASTOperatorType::PROJECTION, cur_parent);
		return LogicalPlanToString(plan->children[0], plan_string, node_id, ql_tree, ql_proj_exp->column_aliases);
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto node = dynamic_cast<LogicalFilter *>(plan.get());
		auto condition = node->ParamsToString();
		auto ql_filter_exp = new DuckASTFilter(condition);
		auto expr = shared_ptr<DuckASTBaseOperator>(ql_filter_exp);
		auto node_id = cur_parent + "_" + node->GetName();
		ql_tree->insert(expr, node_id, DuckASTOperatorType::FILTER, cur_parent);
		return LogicalPlanToString(plan->children[0], plan_string, node_id, ql_tree, column_map);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto node = dynamic_cast<LogicalAggregate *>(plan.get());
		auto par = node->ParamsToString();
		auto names = node->GetName();
		auto binds = node->GetColumnBindings();
		vector<string> group_names;
		int counter = 0;
		for (auto &grp : node->groups) {
			group_names.push_back(grp->GetName());
			// assuming that these bindings are generated in a +2 table_index
			// Need to fix and find actual reason
			auto id = to_string(binds[counter].table_index - 2) + "." + to_string(binds[counter].column_index);
			column_map[id] = grp->GetName();
			counter++;
		}
		vector<string> aggregate_function;
		for (auto &grp : node->expressions) {
			aggregate_function.push_back(grp->GetName());
		}
		auto node_id = cur_parent + "_" + node->GetName();
		auto ql_aggregate_node = new DuckASTAggregate(aggregate_function, group_names);
		auto expr = (shared_ptr<DuckASTBaseOperator>)(ql_aggregate_node);
		expr->name = node_id;
		ql_tree->insert(expr, node_id, DuckASTOperatorType::AGGREGATE, cur_parent);
		return LogicalPlanToString(plan->children[0], plan_string, node_id, ql_tree, column_map);
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto node = dynamic_cast<LogicalOrder *>(plan.get());
		auto ql_order_by = new DuckASTOrderBy();
		for (auto &order : node->orders) {
			auto name = order.expression->GetName();
			string order_type = "";
			switch (order.type) {
			case OrderType::DESCENDING: {
				order_type = "DESC";
				break;
			}
			case OrderType::ASCENDING: {
				order_type = "ASC";
				break;
			}
			}
			ql_order_by->add_order_column(name, order_type);
		}
		auto expr = (shared_ptr<DuckASTBaseOperator>)(ql_order_by);
		auto node_id = cur_parent + "_" + node->GetName();
		expr->name = node_id;
		ql_tree->insert(expr, node_id, DuckASTOperatorType::ORDER_BY, cur_parent);
		return LogicalPlanToString(plan->children[0], plan_string, node_id, ql_tree, column_map);
	}
	case LogicalOperatorType::LOGICAL_GET: {
		auto node = dynamic_cast<LogicalGet *>(plan.get());
		auto ql_get_exp = new DuckASTGet();
		ql_get_exp->name = node->GetName();
		ql_get_exp->table_name = node->GetTable()->name;
		ql_get_exp->all_columns = true;

		auto bindings = node->GetColumnBindings();
		auto column_ids = node->column_ids;
		auto column_names = node->GetTable()->GetColumns().GetColumnNames();
		auto current_table_index = node->GetTableIndex();
		unordered_map<string, string> cur_col_map; // To avoid any changes in ordering of columns
		for (int i = 0; i < bindings.size(); i++) {
			auto cur_binding = bindings[i];
			cur_col_map[to_string(cur_binding.table_index) + "." + to_string(cur_binding.column_index)] =
			    column_names[column_ids[i]];
		}
		unordered_map<string, string> alias_map;
		for (auto curmp : cur_col_map) {
			auto alias = column_map[curmp.first];
			if (alias == curmp.second) {
				alias_map[curmp.second] = "";
			} else {
				alias_map[curmp.second] = alias;
				ql_get_exp->all_columns = false;
			}
		}
		ql_get_exp->alias_map = alias_map;
		if (ql_get_exp->all_columns && alias_map.size() != column_names.size()) {
			ql_get_exp->all_columns = false;
		}

		auto expr = (shared_ptr<DuckASTBaseOperator>)ql_get_exp;
		ql_tree->insert(expr, cur_parent + "_" + ql_get_exp->name, DuckASTOperatorType::GET, cur_parent);
		break;
	}
	case LogicalOperatorType::LOGICAL_INSERT: {
		// we need to handle this case to support IVM properly
		// the plan to be turned into string start with an INSERT
		// which can be ignored when transforming it back to string
		// when LPTS can handle insertions, we can create a new function to remove/ignore the top insert node
		LogicalPlanToString(plan->children[0], plan_string, "", ql_tree, column_map);
	}
	default: {
		auto node = plan.get();
	}
	}
}

} // namespace duckdb
