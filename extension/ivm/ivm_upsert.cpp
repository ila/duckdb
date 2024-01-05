#include "include/ivm_upsert.hpp"

#include "../../compiler/include/compiler_extension.hpp"
#include "../../compiler/include/ivm/ivm_compile_upsert.hpp"
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

	string view_catalog_name = StringValue::Get(parameters.values[0]);
	string view_schema_name = StringValue::Get(parameters.values[1]);
	string view_name = StringValue::Get(parameters.values[2]);

	// debug - this is not found (todo)
	// extracting the query from the view definition
	Connection con(*context.db.get());

	auto delta_view_catalog_entry =
	    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, view_catalog_name, view_schema_name, "delta_" + view_name,
	                     OnEntryNotFound::THROW_EXCEPTION, error_context);
	auto index_delta_view_catalog_entry =
	    catalog.GetEntry(context, CatalogType::INDEX_ENTRY, view_catalog_name, view_schema_name,
	                     view_name + "_ivm_index", OnEntryNotFound::RETURN_NULL, error_context);

	auto view_query_entry = con.Query("select * from _duckdb_ivm_views where view_name = '" + view_name + "';");
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

	switch (view_query_type) {
	case IVMType::AGGREGATE_GROUP: {
		upsert_query = CompileAggregateGroups(view_name, index_delta_view_catalog_entry, column_names);
		break;
	}

	case IVMType::SIMPLE_FILTER:
	case IVMType::SIMPLE_PROJECTION: {
		upsert_query = CompileProjectionsFilters(view_name, column_names);
		break;
	}

	case IVMType::SIMPLE_AGGREGATE: {
		upsert_query = CompileSimpleAggregates(view_name, column_names);
		break;
	}
	}
	// DoIVM is a table function (root of the tree)
	string ivm_query;

	// splitting the query in two to make it easier to turn into string (insertions are the same)
	// ivm_query = "insert into delta_" + view_name + " ";
	string do_ivm = "select * from DoIVM('" + view_catalog_name + "','" + view_schema_name + "','" + view_name + "');";

	con.BeginTransaction();
	Parser p;
	p.ParseQuery(do_ivm);
	Planner planner(*con.context);
	planner.CreatePlan(move(p.statements[0]));
	auto plan = move(planner.plan);
	Optimizer optimizer(*planner.binder, *con.context);
	plan = optimizer.Optimize(move(plan));

	con.Rollback();
	ivm_query += LogicalPlanToString(plan);

	// string select_query = "SELECT * FROM delta_" + view_name + ";";

	// now we delete everything from the delta view
	string delete_from_view_query = "delete from delta_" + view_name + ";";
	// string ivm_result = "select * from " + view_name + ";";
	string ivm_result;

	// todo - delete also from delta table and insert into original table

	// string query = ivm_query + select_query;
	string query = ivm_query + "\n\n" + upsert_query + "\n" + delete_from_view_query + "\n" + ivm_result;

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

	return query;
}

} // namespace duckdb