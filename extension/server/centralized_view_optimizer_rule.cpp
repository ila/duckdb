#include "include/centralized_view_optimizer_rule.hpp"

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/printer.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/planner.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include "duckdb/planner/binder.hpp"

#include <compiler_extension.hpp>
#include <logical_plan_to_string.hpp>
#include <regex>
#include <duckdb/function/aggregate/distributive_function_utils.hpp>
#include <duckdb/function/aggregate/distributive_functions.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <duckdb/planner/expression/bound_aggregate_expression.hpp>
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_comparison_join.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <fmt/format.h>

namespace duckdb {

void CVRewriteRule::CVRewriteRuleFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// the plan variable contains the plan for "SELECT * FROM flush('view_name');"

	// for flush, we need to:
	// 1. insert into the centralized table the columns meeting min agg
	// 2. removing the set from 1) in the materialized view
	// 3. also remove everything with expired TTL
	// note: we query the min agg separately and hardcode it (for performance reasons)

	/* to optimize 1) and 2), we change the metadata column to a dummy value
	 * update centralized_view_$name x
	 * set action = 2
	 * from (
	 *	select c1, c2, c3, win, count(distinct client_id
	 *	from centralized_view_$name
	 *	group by c1, c2, c3, win
	 *	having count(distinct client_id) > min_agg) y
	 * where x.c1 = y.c1 and x.c2 = y.c2 and x.c3 = y.c3 and x.win = y.win;
	 */

	if (plan->children.empty()) {
		return;
	}

	// check if plan contains table function 'flush'
	auto child = plan.get();
	while (!child->children.empty()) {
		child = child->children[0].get();
	}
	if (child->GetName().substr(0, 5) != "FLUSH") {
		return;
	}

	auto child_get = dynamic_cast<LogicalGet *>(child);

	auto view_name = child_get->named_parameters["view_name"].ToString();
	auto minimum_aggregation = std::stoi(child_get->named_parameters["min_agg"].ToString());
	auto window = std::stoi(child_get->named_parameters["window"].ToString());
	auto current_window = std::stoi(child_get->named_parameters["current_window"].ToString());
	auto ttl = std::stoi(child_get->named_parameters["ttl"].ToString());
	int ttl_windows = ttl / window;

	auto centralized_view_name = "rdda_centralized_view_" + view_name;
	auto centralized_table_name = "rdda_centralized_table_" + view_name;

	string file_name = centralized_view_name + "_flush.sql";

	auto centralized_view_catalog_entry = Catalog::GetEntry(input.context, CatalogType::TABLE_ENTRY, "test", "main", centralized_view_name,
										 OnEntryNotFound::RETURN_NULL, QueryErrorContext());

	if (!centralized_view_catalog_entry) {
        throw ParserException("Centralized view not found: " + centralized_view_name);
    }
	string update_query_1 = "update " + centralized_view_name + " x\nset action = 2 \nfrom (\n\tselect ";

	string select_names = "";
	string column_names = "";
	string join_names = "";

	auto &centralized_view_entry = centralized_view_catalog_entry->Cast<TableCatalogEntry>();
	for (auto &column : centralized_view_entry.GetColumns().GetColumnNames()) {
		if (column != "action" && column != "client_id" && column != "generation" && column != "arrival") {
			column_names += column + ", ";
			select_names += "x." + column + ", ";
			join_names += "x." + column + " = y." + column + " \nand ";
		}
	}
	// remove the last comma and space
	column_names = column_names.substr(0, column_names.size() - 2);
	
	update_query_1 += column_names + ", "; // without the alias
	update_query_1 += "count(distinct client_id)\n\t";
	update_query_1 += "from " + centralized_view_name + " \n\t";
	update_query_1 += "group by " + column_names + "\n\t";
	update_query_1 += "having count(distinct client_id) >= " + std::to_string(minimum_aggregation) + ") y \n";
	update_query_1 += "where " + join_names.substr(0, join_names.size() - 6) + ";\n";

	auto insert_query = "insert into " + centralized_table_name + " \nselect * \nfrom " + centralized_view_name + " \nwhere action = 2;\n";
	auto delete_query_1 = "delete from " + centralized_view_name + " \nwhere action = 2;\n";

	// now in the centralized view we only have tuples not meeting the minimum aggregation
	// three options:
	// 1 - TTL not expired -> keep in the view
	// 2 - TTL expired but cv_client_count + ct_client_count >= min_agg -> store in the centralized table
	// 3 - TTL expired and cv_client_count + ct_client_count < min_agg -> remove from the view

	// we also add a where clause to speed up the query
	string x_agg = "with x as (\n\t";
	x_agg += "select " + column_names + ", count(distinct client_id) as client_count \n\t";
	x_agg += "from " + centralized_view_name + " \n\t";
	x_agg += "group by " + column_names + "), \n";
	string y_agg = "y as (\n\t";
	y_agg += "select " + column_names + ", count(distinct client_id) as client_count \n\t";
	y_agg += "from " + centralized_table_name + " \n\t";
	y_agg += "where window >= " + to_string(current_window - ttl_windows) + " \n\t";
	y_agg += "group by " + column_names + ") \n";
	string update_query_2 = x_agg + y_agg;
	update_query_2 += "update " + centralized_view_name + " x \n";
	update_query_2 += "set action = 2 \n";
	update_query_2 += "from x, y \n";
	update_query_2 += "where " + join_names + "x.client_count + y.client_count >= " + to_string(minimum_aggregation) + "\n\t";
	update_query_2 += "(select rdda_min_agg from rdda_view_constraints where view_name = '" + view_name + "');\n";

	// lastly we remove stale tuples
	string delete_query_2 = "delete from " + centralized_view_name + " where rdda_window <= " + to_string(current_window - ttl_windows) + ";\n";

	CompilerExtension::WriteFile(file_name, false, update_query_1);
	CompilerExtension::WriteFile(file_name, true, insert_query);
	CompilerExtension::WriteFile(file_name, true, delete_query_1);
	CompilerExtension::WriteFile(file_name, true, update_query_2);
	CompilerExtension::WriteFile(file_name, true, insert_query);
	CompilerExtension::WriteFile(file_name, true, delete_query_1);
	CompilerExtension::WriteFile(file_name, true, delete_query_2);




}
} // namespace duckdb
