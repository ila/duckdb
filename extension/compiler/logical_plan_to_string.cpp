#include "include/logical_plan_to_string.hpp"
#include "include/data_representation.hpp"

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
	// LogicalPlanToString(plan, plan_string, column_names, column_aliases, "", insert_table_name, false, prj);
	LogicalPlanToString(plan, plan_string, "", prj);
	Printer::Print("Display!-------------");
	prj->displayTree();
	return plan_string;
}

void LogicalPlanToString(unique_ptr<LogicalOperator> &plan, string &plan_string,
	string cur_parent, unique_ptr<DuckAST> &ql_tree) {
	ql_tree->displayTree();
	if(cur_parent.size() == 0) {
		cur_parent = "native";
	}
	switch(plan->type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto node = dynamic_cast<LogicalProjection *>(plan.get());
		auto ql_proj_exp = new DuckASTProjection();
		ql_proj_exp->name = node->GetName();
		auto bindings = node->GetColumnBindings();
		auto expressions = node->expressions;
		for(auto expression: expressions) {
			if(expression->type == ExpressionType::BOUND_REF) {
				auto column = dynamic_cast<BoundColumnRefExpression *>(expression.get());
				auto column_index = to_string(column->binding.column_index);
				auto table_index = to_string(column->binding.table_index);
				auto column_name = column->alias;
				ql_proj_exp->add_column(table_index, column_index, column_name);
			}
		}
		auto expr = (shared_ptr<DuckASTBaseExpression>)(ql_proj_exp);
		ql_tree->insert(expr, cur_parent + "_" + ql_proj_exp->name, DuckASTExpressionType::PROJECTION, cur_parent);
		return LogicalPlanToString(plan->children[0], plan_string, cur_parent, ql_tree);
	}
	case LogicalOperatorType::LOGICAL_GET: {
		auto node = dynamic_cast<LogicalGet *>(plan.get());
		auto ql_get_exp = new DuckASTGet();
	}
	}
}

void LogicalPlanToString_old(unique_ptr<LogicalOperator> &plan, string &plan_string,
                         std::unordered_map<string, string> &column_names,
                         std::vector<std::pair<string, string>> &column_aliases, string cur_parent,
						 string &insert_table_name, bool do_join, unique_ptr<DuckAST> &ql) {

	Printer::Print("Type: ");
	// ql->displayTree();
	Printer::Print(cur_parent);
	if(cur_parent.size() == 0) {
		cur_parent = "native";
	}
	// we reached a root node
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_GET: { // projection
		auto node = dynamic_cast<LogicalGet *>(plan.get());
		auto bindings = node->GetColumnBindings();
		auto table_name = node->GetTable().get()->name;
		auto current_table_index = node->GetTableIndex();
		// Get columns from column names mapq

		auto scan_column_names = node->names;

		auto ql_exp = new DuckASTGet(table_name, current_table_index[0], node->names);
		ql_exp->name = node->GetName();
		if(column_aliases.size() == scan_column_names.size()) {
			ql_exp->column_names = scan_column_names;
		} else {
			ql_exp->all_columns = true;
		}

		ql_exp->table_index = current_table_index[0];
		auto fin_exp = (shared_ptr<DuckASTBaseExpression>)ql_exp;
		ql->insert(fin_exp, cur_parent + "_" + ql_exp->name, DuckASTExpressionType::GET,cur_parent);
		// we don't need the table function here; we assume it is a simple scan
		string from_string = "";

		if(!do_join){
			from_string = "from " + table_name + "\n";
		}

		// now let's see if the scan has any filters
		std::vector<string> filters;
		for (auto &filter : node->table_filters.filters) {
			// extract the column id of this filter
			auto id = filter.first;
			auto column_name = scan_column_names[id];
			filters.emplace_back(filter.second->ToString(column_name));
		}

		// SELECT sname, bname from sailors cross join boats;

		// column_aliases = [sname, bname]

		// column bindings: 0.0, 0.1, 0.2
		// we are (probably) at the bottom
		if (plan->children.empty()) {
			// add the select statement
			string select_string = "select ";
			// SELECT * case - all the columns of the table appear in the column aliases (second of the pair)
			// we assume it's a SELECT * if the columns are the same in the same order
			bool select_all = false;
			// if the plan string does not start with "group by" it can be a SELECT *
			// todo - change this when the queries become more complicated
			if (plan_string.substr(0, 8) != "group by") {
				auto table_column_names = node->GetTable()->GetColumns().GetColumnNames();
				if (column_aliases.size() == table_column_names.size()) {
					// we need to further check if the column names and their order are the same
					for (auto i = 0; i < table_column_names.size(); i++) {
						if (column_aliases[i].first != table_column_names[i]) {
							break;
						}
					}
					select_all = true;
				}
			}
			// now we sort out the column aliases
			// edge case: when we have a projection without group, the second element will be the placeholder
			if (!select_all) {
				for (auto &pair : column_aliases) {
					if (pair.first == pair.second || pair.second == "duckdb_placeholder_internal") {
						select_string = select_string + pair.first + " ";
					} else {
						select_string = select_string + pair.second + " as " + pair.first + ", ";
					}
				}
				// erase the last comma and space
				select_string.erase(select_string.size() - 2, 2);
			} else {
				select_string = select_string + "*";
			}
			select_string += "\n";
			// now construct the WHERE clause
			string where_string;
			if (!filters.empty()) {
				where_string = "where ";
				for (auto &filter : filters) {
					where_string += filter + " and ";
				}
				// trim the last " and "
				where_string.erase(where_string.size() - 5, 5);
				where_string += "\n";
			}
			// plan string is the group by
			string insert_string;
			if (!insert_table_name.empty()) {
				insert_string = "insert into " + insert_table_name + "\n";
			}
			plan_string = insert_string + select_string + from_string + where_string + plan_string;
			plan_string += ";";
			return;
		} else {
			// uh oh
		}


	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// we cannot use ParamsToString here because we lose the aliases
		auto node = dynamic_cast<LogicalAggregate *>(plan.get());
		// we only support SUM and COUNT, so we only search for these
		auto bindings = node->GetColumnBindings(); // 2.0, 3.0, 3.1
		// now we have to extract the old table indexes, contained in groups and expressions
		std::vector<std::pair<string, string>> aggregate_aliases;
		// this is probably unnecessary but helps code readability
		// we want all the old bindings to be in the same place
		// we iterate groups first, then expressions
		auto first = true;
		for (auto &group : node->groups) {
			auto column = dynamic_cast<BoundColumnRefExpression *>(group.get());
			aggregate_aliases.emplace_back(std::to_string(column->binding.table_index) + "." +
			                                   std::to_string(column->binding.column_index),
			                               column->alias);
			if (first) {
				plan_string += "group by " + column->alias;
				first = false;
			} else {
				plan_string += ", " + column->alias;
			}
		}
		for (size_t i = 0; i < node->expressions.size(); i++) {
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
						// todo error handling
					}
				} else {
					// we are in the count_star() case
					// this does not get bindings - the columns in the scan might be less
					// put a temporary placeholder
					aggregate_aliases.emplace_back("-1.-1", "count(*)");
				}
			}
		}

		// now we should replace old bindings with new ones in the aggregate aliases
		for (idx_t i = 0; i < bindings.size(); i++) {
			auto key = std::to_string(bindings[i].table_index) + "." + std::to_string(bindings[i].column_index);
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
				// error
			}
		}

		// plan_string += "\n";
		return LogicalPlanToString_old(plan->children[0], plan_string, column_names, column_aliases, cur_parent, insert_table_name, false, ql);
	}

	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto node = dynamic_cast<LogicalProjection *>(plan.get());

		auto exp = (new DuckASTProjection());
		if (column_aliases.empty()) {
			// this is the first projection (at least in this first rudimentary implementation)
			// when we support more complicated queries, we can just add a do_ivm flag to skip this projection
			// (our optimizer rule adds a "fake" projection at the bottom)
			auto bindings = node->GetColumnBindings(); // not really needed now
			for (auto &expression : node->expressions) {
				// todo handle the cases of other expression types

				if (expression->type == ExpressionType::BOUND_COLUMN_REF) {
					auto column = dynamic_cast<BoundColumnRefExpression *>(expression.get());
					auto column_index = column->binding.column_index;
					auto table_index = column->binding.table_index;
					auto column_name = column->alias;

					// exp->add_column(column_name);

					column_names[std::to_string(table_index) + "." + std::to_string(column_index)] = column_name;
					// we use a placeholder to figure out which column names have been aliased
					column_aliases.emplace_back(column_name, "duckdb_placeholder_internal");
				}
			}
		}
		auto fin_exp = (shared_ptr<DuckASTBaseExpression>)exp;
		auto cur_id = cur_parent + "_" + node->GetName();
		ql->insert(fin_exp, cur_parent + "_" + node->GetName(), DuckASTExpressionType::PROJECTION, cur_parent);
		return LogicalPlanToString_old(plan->children[0], plan_string, column_names, column_aliases, cur_id, insert_table_name, false, ql);
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		// basically the same logic as the logical get
		auto node = dynamic_cast<LogicalFilter *>(plan.get());
		plan_string = "where " + node->ParamsToString() + "\n" + plan_string;
		return LogicalPlanToString_old(plan->children[0], plan_string, column_names, column_aliases, "", insert_table_name, false, ql);
	}
	case LogicalOperatorType::LOGICAL_INSERT: {
		// this should be transformed into an upsert
		auto node = dynamic_cast<LogicalInsert *>(plan.get());
		return LogicalPlanToString_old(plan->children[0], plan_string, column_names, column_aliases, "", node->table.name, false, ql);
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// Tacle Statements Like
		// SELECT titanic.*, titanic2.* FROM titanic CROSS JOIN titanic2;
		auto node = dynamic_cast<LogicalCrossProduct* >(plan.get());
		auto table = node->GetTableIndex();
		auto &children = node->children;
		auto right_projection = dynamic_cast<LogicalGet *>(children.back().get());
		// auto left_projection = dynamic_cast<LogicalGet *>(children.front().get());
		auto right_table_name = right_projection->GetTable().get()->name;
		plan_string = plan_string + "cross join " + right_table_name;
		LogicalPlanToString_old(plan->children[0], plan_string, column_names, column_aliases, "", insert_table_name, false, ql);
		return LogicalPlanToString_old(plan->children[1], plan_string, column_names, column_aliases, "", insert_table_name, true, ql);
	}
	// case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
	// 	// Tackles Statements like
	// 	// SELECT a.name, b.name FROM a JOIN b ON a.name = b.name;
	// 	auto node = dynamic_cast<LogicalComparisonJoin *>(plan.get());	
	// 	plan_string = "join " + plan_string;
	// 	// auto exp_bnds = node->GetExpressionBindings();
	// }
	}
}

} // namespace duckdb
