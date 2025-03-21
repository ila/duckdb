#include "include/openivm_upsert.hpp"

#include "../../compiler/include/compiler_extension.hpp"
#include "../../compiler/include/openivm/openivm_compile_upsert.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/planner.hpp"
#include "logical_plan_to_string.hpp"

namespace duckdb {

string UpsertDeltaQueries(ClientContext &context, const FunctionParameters &parameters) {
	// queries to run in order to materialize IVM upserts
	// these are executed whenever the pragma ivm_upsert is called
	auto &catalog = Catalog::GetSystemCatalog(context);
	QueryErrorContext error_context = QueryErrorContext();

	string view_catalog_name;
	string view_schema_name;
	string attached_db_catalog_name;
	string attached_db_schema_name;
	string view_name;
	bool cross_system = false; // to make checks easier
	// if we are in a cross-system scenario, the tables need to be stored separately
	// ex. the delta tables are on the attached database, while the delta views on DuckDB

	// extracting the query from the view definition
	Connection con(*context.db.get());

	if (parameters.values.size() == 3) {
		// ivm_options was called, so different schema and catalog
		view_catalog_name = StringValue::Get(parameters.values[0]);
		view_schema_name = StringValue::Get(parameters.values[1]);
		view_name = StringValue::Get(parameters.values[2]);
	} else if (parameters.values.size() == 5) {
		// ivm_cross_system was called, so different schema and catalog
		view_catalog_name = StringValue::Get(parameters.values[0]);
		view_schema_name = StringValue::Get(parameters.values[1]);
		attached_db_catalog_name = StringValue::Get(parameters.values[2]);
		attached_db_schema_name = StringValue::Get(parameters.values[3]);
		view_name = StringValue::Get(parameters.values[4]);
		cross_system = true;
	} else {
		// simple ivm, we assume current schema and catalog
		view_catalog_name = con.Query("select current_catalog();")->GetValue(0, 0).ToString();
		view_schema_name = con.Query("select current_schema();")->GetValue(0, 0).ToString();
		view_name = StringValue::Get(parameters.values[0]);
	}

	auto delta_view_catalog_entry =
	    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, view_catalog_name, view_schema_name, "delta_" + view_name,
	                     OnEntryNotFound::THROW_EXCEPTION, error_context);
	auto index_delta_view_catalog_entry =
	    catalog.GetEntry(context, CatalogType::INDEX_ENTRY, view_catalog_name, view_schema_name,
	                     view_name + "_ivm_index", OnEntryNotFound::RETURN_NULL, error_context);

	auto view_query_entry = con.Query("select * from _duckdb_ivm_views where view_name = '" + view_name + "';");
	if (view_query_entry->HasError()) {
		throw ParserException("Error while querying view definition");
	}
	if (view_query_entry->RowCount() == 0) {
		throw ParserException("View not found! Please call IVM with a materialized view.");
	}
	auto view_query_type_data = view_query_entry->GetValue(2, 0);
	IVMType view_query_type = static_cast<IVMType>(view_query_type_data.GetValue<int8_t>());

	// we cannot use column references in ART indexes since their implementation is really messy
	// we need to use column indexes; maybe there is a more efficient way (unique constraints?)
	// but I cannot be bothered to think about this now

	// note: joins are hash joins by default, with group hash (try forcing index joins?)

	// first of all we need to understand the keys
	auto delta_view_entry = dynamic_cast<TableCatalogEntry *>(delta_view_catalog_entry.get());
	// compiler is too stupid to figure out "auto" here
	const ColumnList &delta_view_columns = delta_view_entry->GetColumns();

	auto column_names = delta_view_columns.GetColumnNames();

	string upsert_query;

	// this is to compile the query to merge the materialized view with its delta version
	// depending on the query type, this procedure will be done differently
	// aggregates require an upsert query, while simple filters and projections are an insert
	switch (view_query_type) {
	case IVMType::AGGREGATE_GROUP: {
		upsert_query = CompileAggregateGroups(view_name, index_delta_view_catalog_entry, column_names);
		break;
	}

	// note: simple_filter removed 2024-12-11
	case IVMType::SIMPLE_PROJECTION: {
		upsert_query = CompileProjectionsFilters(view_name, column_names);
		break;
	}

	case IVMType::SIMPLE_AGGREGATE: {
		upsert_query = CompileSimpleAggregates(view_name, column_names);
		break;
	}
		// todo joins
	}
	// DoIVM is a table function (root of the tree)
	string ivm_query;

	// splitting the query in two to make it easier to turn into string (insertions are the same)
	// ivm_query = "insert into delta_" + view_name + " ";
	string do_ivm = "select * from DoIVM('" + view_catalog_name + "','" + view_schema_name + "','" + view_name + "');";

	// we need to check if the view is in fact a MV
	con.BeginTransaction();
	// we need the table names since we need to update the metadata tables
	auto tables = con.Query("select table_name from _duckdb_ivm_delta_tables where view_name = '" + view_name + "';");

	if (tables->HasError()) {
		throw ParserException("Error while querying _duckdb_ivm_delta_tables");
	}

	// now we can plan the query
	Parser p;
	p.ParseQuery(do_ivm);

	Planner planner(*con.context);
	planner.CreatePlan(move(p.statements[0]));
	auto plan = move(planner.plan);
	Optimizer optimizer(*planner.binder, *con.context);
	plan = optimizer.Optimize(move(plan)); // this transforms the plan into an incremental plan

	con.Rollback();

	ivm_query += LogicalPlanToString(context, plan); // we turn the plan into a string

	// we delete everything from the delta view (we don't need the data anymore, it will be inserted in the view)
	string delete_from_view_query = "delete from delta_" + view_name + ";";
	string ivm_result;

	// now we can also delete from the delta table, but only if all the dependent views have been refreshed
	// example: if two views A and B are on the same table T, we can only remove tuples from T
	// if both A and B have been refreshed (up to some timestamp)
	// to check this, we extract the minimum timestamp from _duckdb_ivm_delta_tables
	string delete_from_delta_table_query;
	// firstly we reset the timestamp
	string update_timestamp_query =
	    "update _duckdb_ivm_delta_tables set last_update = now() where view_name = '" + view_name + "';\n";

	for (size_t i = 0; i < tables->RowCount(); i++) {
		auto table_name = tables->GetValue(0, i).ToString();
		if (cross_system) {
			table_name = attached_db_catalog_name + "." + attached_db_schema_name + "." + table_name;
		}
		// now we delete anything we don't need anymore
		delete_from_delta_table_query += "delete from " + table_name +
		                                 " where _duckdb_ivm_timestamp < (select min(last_update) from "
		                                 "_duckdb_ivm_delta_tables where table_name = '" +
		                                 table_name + "');\n";
	}

	string query = ivm_query + "\n\n" + update_timestamp_query + "\n" + upsert_query + "\n" + delete_from_view_query +
	               "\n" + ivm_result + "\n" + delete_from_delta_table_query;

	// now also compiling the queries for future usage
	string db_path;
	if (!context.db->config.options.database_path.empty()) {
		db_path = context.db->GetFileSystem().GetWorkingDirectory();
	} else {
		Value db_path_value;
		context.TryGetCurrentSetting("ivm_files_path", db_path_value);
		db_path = db_path_value.ToString();
	}
	string ivm_file_path = db_path + "/ivm_upsert_queries_" + view_name + ".sql";
	duckdb::CompilerExtension::WriteFile(ivm_file_path, false, query);

	Value execute;
	context.TryGetCurrentSetting("execute", execute);
	// (the "execute" flag is only for benchmarking purposes)
	// if the database is in memory, we do not want to run the whole IVM thing
	// so we return a dummy query (we must return something here)
	if (!context.db->config.options.database_path.empty() &&
	    (execute.IsNull() || execute.GetValue<bool>())) { // in memory
		return query;
	} else {
		return "select 1"; // dummy query
	}
}

} // namespace duckdb
