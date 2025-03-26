#include "include/flush_function.hpp"

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/printer.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/planner.hpp>
#include "duckdb/planner/binder.hpp"

#include <common.hpp>
#include <compiler_extension.hpp>
#include <logical_plan_to_string.hpp>
#include <regex>
#include <duckdb/common/local_file_system.hpp>
#include <duckdb/function/aggregate/distributive_functions.hpp>
#include <duckdb/main/database.hpp>
#include <fmt/format.h>

namespace duckdb {

void FlushFunction(ClientContext &context, const FunctionParameters &parameters) {

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

	string config_path = "../extension/server/";
	string config_file = "server.config";
	auto config = ParseConfig(config_path, config_file);

	string server_db_name = config["db_name"];
	string client_catalog_name = "rdda_client";
	string client_db_name = "rdda_client.db";
	string metadata_db_name = "rdda_parser.db";
	DuckDB server_db(server_db_name);
	DuckDB client_db(client_db_name);
	DuckDB metadata_db(metadata_db_name);
	Connection server_con(server_db);
	Connection client_con(client_db);
	Connection metadata_con(metadata_db);

	auto view_name = StringValue::Get(parameters.values[0]);
	auto centralized_view_name = "rdda_centralized_view_" + view_name;
	auto centralized_table_name = "rdda_centralized_table_" + view_name;

	LocalFileSystem fs;

	string file_name = centralized_view_name + "_flush.sql";
	// todo - this file is recomputed every time, we should store it
	// but if we store it then we have to extract window and ttl with sql
	// if (fs.FileExists(file_name)) {
	// 	auto queries = CompilerExtension::ReadFile(file_name);
	// 	con.Query(queries);
	// 	return;
	// }
	string min_agg_query =
	    "select rdda_min_agg, rdda_window, rdda_ttl from rdda_view_constraints where view_name = '" + view_name + "';";
	auto r = metadata_con.Query(min_agg_query);
	if (r->HasError()) {
		throw ParserException("Error while querying columns metadata: " + r->GetError());
	}
	if (r->RowCount() == 0) {
		throw ParserException("View metadata not found!");
	}
	auto minimum_aggregation = std::stoi(r->GetValue(0, 0).ToString());
	auto window = std::stoi(r->GetValue(1, 0).ToString());
	auto ttl = std::stoi(r->GetValue(2, 0).ToString());

	string current_window_query =
	    "select rdda_window from rdda_current_window where view_name = 'rdda_centralized_view_" + view_name + "';";
	r = metadata_con.Query(current_window_query);
	if (r->HasError()) {
		throw ParserException("Error while querying window metadata: " + r->GetError());
	}
	if (r->RowCount() == 0) {
		throw ParserException("Window metadata not found!");
	}
	auto current_window = std::stoi(r->GetValue(0, 0).ToString());
	int ttl_windows = ttl / window;

	client_con.BeginTransaction();
	auto centralized_view_catalog_entry =
	    Catalog::GetEntry(*client_con.context, CatalogType::TABLE_ENTRY, client_catalog_name, "main", centralized_view_name,
	                      OnEntryNotFound::RETURN_NULL, QueryErrorContext());
	client_con.Rollback();

	if (!centralized_view_catalog_entry) {
		throw ParserException("Centralized view not found: " + centralized_view_name);
	}
	centralized_view_name = "rdda_client." + centralized_view_name; // since we are attaching the db
	centralized_table_name = server_db_name.substr(0, server_db_name.size() - 3) + "." + centralized_table_name;
	string update_query_1 = "update " + centralized_view_name + " x\nset action = 2 \nfrom (\n\tselect ";

	string protected_column_names = "";
	string join_names = "";
	string join_names_cte = "";
	string table_column_names = ""; // column names of the centralized table (without metadata)

	// now we need to query the protected columns
	string view_query = "select query from rdda_tables where name = '" + view_name + "';";
	auto view_query_result = metadata_con.Query(view_query);
	if (view_query_result->HasError()) {
		throw ParserException("Error while querying view definition: " + view_query_result->GetError());
	}
	// todo - should we not use parser internal here?
	metadata_con.BeginTransaction();
	auto table_names = metadata_con.GetTableNames(view_query_result->GetValue(0, 0).ToString());
	metadata_con.Rollback();
	// currently there is only one table name, but might be extended in the future
	string in_table_names = "(";
	for (auto &table : table_names) {
		in_table_names += "'" + table + "', ";
	}
	in_table_names = in_table_names.substr(0, in_table_names.size() - 2) + ")";

	auto protected_columns_query =
	    "select column_name from rdda_table_constraints where rdda_protected = 1 and table_name in " + in_table_names +
	    ";";
	auto protected_columns = metadata_con.Query(protected_columns_query);
	if (protected_columns->HasError()) {
		throw ParserException("Error while querying protected columns: " + protected_columns->GetError());
	}
	for (size_t i = 0; i < protected_columns->RowCount(); i++) {
		auto column = protected_columns->GetValue(0, i).ToString();
		protected_column_names += column + ", ";
		join_names += "x." + column + " = y." + column + " \nand ";
		join_names_cte += "x." + column + " = z." + column + " \nand ";
	}

	// also adding the window
	protected_column_names += "rdda_window";
	join_names += "x.rdda_window = y.rdda_window \nand ";
	join_names_cte += "x.rdda_window = z.rdda_window \nand ";

	auto &centralized_view_entry = centralized_view_catalog_entry->Cast<TableCatalogEntry>();
	for (auto &column : centralized_view_entry.GetColumns().GetColumnNames()) {
		if (column != "action" && column != "generation" && column != "arrival") {
			table_column_names += column + ", ";
		}
	}
	// remove the last comma and space
	table_column_names = table_column_names.substr(0, table_column_names.size() - 2);

	update_query_1 += protected_column_names + ", "; // without the alias
	update_query_1 += "count(distinct client_id)\n\t";
	update_query_1 += "from " + centralized_view_name + " \n\t";
	update_query_1 += "group by " + protected_column_names + "\n\t";
	update_query_1 += "having count(distinct client_id) >= " + std::to_string(minimum_aggregation) + ") y \n";
	update_query_1 += "where " + join_names.substr(0, join_names.size() - 6) + ";\n\n";

	// we have to attach the client to the server and not the other way around
	// because the server database has to be the default for IVM pipelines
	auto attach_query = "attach '" + client_db_name + "' as rdda_client;\n\n";
	auto attach_query_read_only = "attach '" + client_db_name + "' as rdda_client (read_only);\n\n";
	auto insert_query = "insert into " + centralized_table_name + " \nselect " + table_column_names + " \nfrom " +
	                    centralized_view_name + " \nwhere action = 2;\n\n";
	auto delete_query_1 = "delete from " + centralized_view_name + " \nwhere action = 2;\n\n";

	// now in the centralized view we only have tuples not meeting the minimum aggregation
	// three options:
	// 1 - TTL not expired -> keep in the view
	// 2 - TTL expired but cv_client_count + ct_client_count >= min_agg -> store in the centralized table
	// 3 - TTL expired and cv_client_count + ct_client_count < min_agg -> remove from the view

	// we also add a where clause to speed up the query
	string x_agg = "with x as (\n\t";
	x_agg += "select " + protected_column_names + ", count(distinct client_id) as client_count \n\t";
	x_agg += "from " + centralized_view_name + " \n\t";
	x_agg += "group by " + protected_column_names + "), \n";
	string y_agg = "y as (\n\t";
	y_agg += "select " + protected_column_names + ", count(distinct client_id) as client_count \n\t";
	y_agg += "from " + centralized_table_name + " \n\t";
	y_agg += "where rdda_window >= " + to_string(current_window - ttl_windows) + " \n\t";
	y_agg += "group by " + protected_column_names + ") \n";
	string update_query_2 = x_agg + y_agg;
	update_query_2 += "update " + centralized_view_name + " z \n";
	update_query_2 += "set action = 2 \n";
	update_query_2 += "from x, y \n";
	update_query_2 += "where " + join_names + join_names_cte +
	                  "x.client_count + y.client_count >= " + to_string(minimum_aggregation) + ";\n\n";
	string detach_query = "detach rdda_client;\n\n";
	// lastly we remove stale tuples
	string delete_query_2 = "delete from " + centralized_view_name +
	                        " where rdda_window <= " + to_string(current_window - ttl_windows) + ";\n\n";

	// IVM delta propagation opens another connection to the client database
	// to avoid concurrency issues, every time we might trigger IVM (insertion into a centralized table)
	// we detach the client database and reattach it as read-only
	auto queries = attach_query + update_query_1 + detach_query + attach_query_read_only + insert_query + detach_query
		+ attach_query + delete_query_1 + update_query_2 + detach_query + attach_query_read_only + insert_query
		+ detach_query + attach_query + delete_query_1 + delete_query_2 + detach_query;

	ExecuteCommitAndWriteQueries(server_con, queries, file_name, false);
}
} // namespace duckdb
