#include "include/flush_function.hpp"
#include "include/update_metrics.hpp"

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

int run = 0; // benchmark run (only for benchmarking purposes)

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

	auto database = StringValue::Get(parameters.values[1]);
	if (database != "duckdb" && database != "postgres") {
		throw ParserException("Invalid database type: " + database + " - only duckdb and postgres are supported!");
	}

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

	if (fs.FileExists(file_name)) {
		// ExecuteCommitAndWriteQueries(server_con, queries, file_name, false, false);
		bool append = false;
		if (run > 0) {
			append = true;
		}
		string queries = CompilerExtension::ReadFile(file_name);
		ExecuteCommitLogAndWriteQueries(server_con, queries, file_name, view_name, append, run, false);
	 	return;
	 }

	client_con.BeginTransaction();
	auto centralized_view_catalog_entry =
	    Catalog::GetEntry<TableCatalogEntry>(*client_con.context, client_catalog_name, "main",
	                      centralized_view_name, OnEntryNotFound::RETURN_NULL, QueryErrorContext());
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

	string extract_metadata = "WITH stats AS (\n"
						      "\tSELECT rdda_ttl FROM rdda_parser.rdda_view_constraints\n"
	                          "\tWHERE view_name = '" + view_name + "'),\n"
							  "current_window AS (\n"
							  "\tSELECT rdda_window FROM rdda_parser.rdda_current_window\n"
							  "\tWHERE view_name = '" + view_name + "'),\n"
					          "\tthreshold_window AS (\n"
	                          "\tSELECT (cw.rdda_window - s.rdda_ttl) AS expired_window\n"
							  "\tFROM current_window cw, stats s)";

	string min_agg_query =
		"select rdda_min_agg from rdda_view_constraints where view_name = '" + view_name + "';";
	auto r = metadata_con.Query(min_agg_query);
	if (r->HasError()) {
		throw ParserException("Error while querying columns metadata: " + r->GetError());
	}
	if (r->RowCount() == 0) {
		throw ParserException("View metadata not found!");
	}
	auto minimum_aggregation = std::stoi(r->GetValue(0, 0).ToString());

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
	if (minimum_aggregation > 1) {
		update_query_1 += "having count(distinct client_id) >= " + std::to_string(minimum_aggregation);
	}
	update_query_1 += ") y\n";
	update_query_1 += "where " + join_names.substr(0, join_names.size() - 6) + ";\n\n";

	// we have to attach the client to the server and not the other way around
	// because the server database has to be the default for IVM pipelines
	string attach_query;
	string attach_parser_read_only = "attach 'rdda_parser.db' as rdda_parser (read_only);\n\n";
	if (database == "duckdb") {
		attach_query = "attach '" + client_db_name + "' as rdda_client;\n\n";
	} else if (database == "postgres") {
		// todo - extend config to support postgres
		// with postgres, we do not detach the database (it is slow to attach, and the db supports concurrency)
		string postgres_credentials = "'dbname=rdda_client user=ubuntu password=test host=localhost'";
		attach_query = "attach if not exists " + postgres_credentials + " as rdda_client (type postgres);\n\n";
	}
	auto attach_query_read_only = "attach '" + client_db_name + "' as rdda_client (read_only);\n\n";
	auto insert_query = "insert into " + centralized_table_name + " by name\nselect " + table_column_names +
	                    " \nfrom " + centralized_view_name + " \nwhere action = 2;\n\n";
	auto delete_query_1 = "delete from " + centralized_view_name + " \nwhere action = 2;\n\n";

	string detach_query = "detach rdda_client;\n\n";
	// lastly we remove stale tuples
	string delete_query_2 = extract_metadata + "\ndelete from " + centralized_view_name +
	                        " where rdda_window <= (SELECT expired_window FROM threshold_window);\n\n";

	// now generating the queries to update the metadata
	string update_responsiveness = UpdateResponsiveness(view_name);
	string update_completeness = attach_parser_read_only + extract_metadata + UpdateCompleteness(view_name);
	string update_buffer_size = UpdateBufferSize(view_name);
	string cleanup_expired_clients = CleanupExpiredClients(config);

	// IVM delta propagation opens another connection to the client database
	// to avoid concurrency issues, every time we might trigger IVM (insertion into a centralized table)
	// we detach the client database and reattach it as read-only
	string queries;
	if (database == "duckdb") {
		queries = attach_query + update_query_1 + detach_query + attach_query_read_only + insert_query + detach_query +
		          attach_query + delete_query_1 + detach_query + cleanup_expired_clients + update_responsiveness +
		          attach_query + update_completeness + delete_query_2 + update_buffer_size + detach_query;

	} else if (database == "postgres") {
		queries = attach_query + update_query_1 + insert_query + delete_query_1 + cleanup_expired_clients +
		          update_responsiveness + update_completeness + delete_query_2 + update_buffer_size;
	}

	// ExecuteCommitAndWriteQueries(server_con, queries, file_name, false, true);
	bool append = false;
	if (run > 0) {
        append = true;
    }
	ExecuteCommitLogAndWriteQueries(server_con, queries, file_name, view_name, append, run, true);
	run++; // todo - log responsiveness, completeness and buffer size
}

} // namespace duckdb
