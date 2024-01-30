#include "include/logical_plan_to_string.hpp"

#include "postgres_scanner.hpp"

namespace duckdb {

string LogicalPlanToString(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		throw NotImplementedException("Cannot print logical plan with debug_print_bindings enabled");
	}
#endif

	// we can assume that the query is lowercase and return a lowercase query

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
	// now we can call the recursive function
	LogicalPlanToString(context, plan, plan_string, column_names, column_aliases, insert_table_name);
	return plan_string;
}

void LogicalPlanToString(ClientContext &context, unique_ptr<LogicalOperator> &plan, string &plan_string,
                         std::unordered_map<string, string> &column_names,
                         std::vector<std::pair<string, string>> &column_aliases, string &insert_table_name) {

	// we reached a root node
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_GET: { // projection
		auto node = dynamic_cast<LogicalGet *>(plan.get());
		string table_name;
		if (node->GetTable().get()) { // DuckDB table scan
			table_name = node->GetTable().get()->name;
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
			table_name = catalog_schema + dynamic_cast<PostgresBindData *>(node->bind_data.get())->table_name;
		}
		auto scan_column_names = node->names;
		// we don't need the table function here; we assume it is a simple scan
		string from_string = "from " + table_name + "\n";
		// now let's see if the scan has any filters
		std::vector<string> filters;
		for (auto &filter : node->table_filters.filters) {
			// extract the column id of this filter
			auto id = filter.first;
			auto column_name = scan_column_names[id];
			filters.emplace_back(filter.second->ToString(column_name));
		}

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
				if (node->GetTable()) { // todo refactor this with AST
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
			}
			// now we sort out the column aliases
			// edge case: when we have a projection without group, the second element will be the placeholder
			if (!select_all) {
				for (auto &pair : column_aliases) {
					if (pair.first == pair.second || pair.second == "duckdb_placeholder_internal") {
						select_string = select_string + pair.first + ", ";
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
					// we only support "sum" and "count" (this is because IVM is only implemented for these)
					// but we should definitely extend this to all aggregate functions
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
		return LogicalPlanToString(context, plan->children[0], plan_string, column_names, column_aliases, insert_table_name);
	}

	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto node = dynamic_cast<LogicalProjection *>(plan.get());
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
					column_names[std::to_string(table_index) + "." + std::to_string(column_index)] = column_name;
					// we use a placeholder to figure out which column names have been aliased
					column_aliases.emplace_back(column_name, "duckdb_placeholder_internal");
				}
			}
		}
		return LogicalPlanToString(context, plan->children[0], plan_string, column_names, column_aliases, insert_table_name);
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		// basically the same logic as the logical get
		auto node = dynamic_cast<LogicalFilter *>(plan.get());
		// ParamsToString() returns the filter expression
		plan_string = "where " + node->ParamsToString() + "\n" + plan_string;
		return LogicalPlanToString(context, plan->children[0], plan_string, column_names, column_aliases, insert_table_name);
	}
	case LogicalOperatorType::LOGICAL_INSERT: {
		// ignore inserts for now
		auto node = dynamic_cast<LogicalInsert *>(plan.get());
		return LogicalPlanToString(context, plan->children[0], plan_string, column_names, column_aliases, node->table.name);
	}
	}
}

} // namespace duckdb
