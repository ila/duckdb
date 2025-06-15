#define DUCKDB_EXTENSION_MAIN

#include "openivm_extension.hpp"

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/planner.hpp"
#include "openivm_benchmark.hpp"
#include "openivm_cross_system_demo.hpp"
#include "openivm_upsert.hpp"

#include <map>
#include <stdio.h>

namespace duckdb {

struct DoIVMData : public GlobalTableFunctionState {
	DoIVMData() : offset(0) {
	}
	idx_t offset;
	string view_name;
};

struct DoIVMBenchmarkData : public GlobalTableFunctionState {
	DoIVMBenchmarkData() : offset(0) {
	}
	idx_t offset;
};

struct DoIVMDemoData : public GlobalTableFunctionState {
	DoIVMDemoData() : offset(0) {
	}
	idx_t offset;
};

unique_ptr<GlobalTableFunctionState> DoIVMInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DoIVMData>();
	return std::move(result);
}

static unique_ptr<TableRef> DoIVM(ClientContext &context, TableFunctionBindInput &input) {
	return nullptr;
}

static duckdb::unique_ptr<FunctionData> DoIVMBenchmarkBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	// called when the pragma is executed
	// specifies the output format of the query (columns)
	// display the outputs (do not remove)

	if (input.inputs.size() == 2) {
		// lineitem benchmark
		auto scale_factor = DoubleValue::Get(input.inputs[0]);
		auto new_scale_factor = DoubleValue::Get(input.inputs[1]);
		input.named_parameters["scale_factor"] = scale_factor;
		input.named_parameters["new_scale_factor"] = new_scale_factor;
		RunIVMLineitemBenchmark(scale_factor, new_scale_factor);
	} else {
		auto scale_factor = DoubleValue::Get(input.inputs[0]);
		auto insertions = IntegerValue::Get(input.inputs[1]);
		auto deletions = IntegerValue::Get(input.inputs[2]);
		auto updates = IntegerValue::Get(input.inputs[3]);
		input.named_parameters["scale_factor"] = scale_factor;
		input.named_parameters["insertions"] = insertions;
		input.named_parameters["deletions"] = deletions;
		input.named_parameters["updates"] = updates;
		if (insertions < 100 && updates < 100 && deletions < 100) {
			throw NotImplementedException("Error: invalid benchmark parameters.");
		}
		int tuples = scale_factor; // casting
		RunIVMGroupsBenchmark(tuples, insertions, updates, deletions);
	}

	// create result set using column bindings returned by the planner
	auto result = make_uniq<DoIVMBenchmarkFunctionData>();

	// add the multiplicity column
	return_types.emplace_back(LogicalTypeId::BOOLEAN);
	names.emplace_back("Done");

	return std::move(result);
}

static duckdb::unique_ptr<FunctionData> DoIVMDemoBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	// called when the pragma is executed
	// specifies the output format of the query (columns)
	// display the outputs (do not remove)
	auto catalog_name = StringValue::Get(input.inputs[0]);
	auto schema_name = StringValue::Get(input.inputs[1]);
	auto database_name = StringValue::Get(input.inputs[2]);
	auto path = StringValue::Get(input.inputs[3]);

	input.named_parameters["catalog_name"] = catalog_name;
	input.named_parameters["schema_name"] = schema_name;
	input.named_parameters["database_name"] = database_name;
	input.named_parameters["path"] = path;

	RunIVMCrossSystemDemo(catalog_name, schema_name, database_name, path);

	// create result set using column bindings returned by the planner
	auto result = make_uniq<DoIVMBenchmarkFunctionData>();

	// add the multiplicity column
	return_types.emplace_back(LogicalTypeId::BOOLEAN);
	names.emplace_back("Done");

	return std::move(result);
}

static void DoIVMFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = dynamic_cast<DoIVMData &>(*data_p.global_state);
	if (data.offset >= 1) {
		// finished returning values
		return;
	}
	return;
}

unique_ptr<GlobalTableFunctionState> DoIVMBenchmarkInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DoIVMBenchmarkData>();
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> DoIVMDemoInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DoIVMDemoData>();
	return std::move(result);
}

static unique_ptr<TableRef> DoIVMBenchmark(ClientContext &context, TableFunctionBindInput &input) {
	return nullptr;
}

static unique_ptr<TableRef> DoIVMDemo(ClientContext &context, TableFunctionBindInput &input) {
	return nullptr;
}

static duckdb::unique_ptr<FunctionData> DoIVMBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	// called when the pragma is executed
	// specifies the output format of the query (columns)
	// display the outputs (do not remove)
	string view_catalog_name = StringValue::Get(input.inputs[0]);
	string view_schema_name = StringValue::Get(input.inputs[1]);
	string view_name = StringValue::Get(input.inputs[2]);
#ifdef DEBUG
	printf("View to be incrementally maintained: %s \n", view_name.c_str());
#endif

	input.named_parameters["view_name"] = view_name;
	input.named_parameters["view_catalog_name"] = view_catalog_name;
	input.named_parameters["view_schema_name"] = view_schema_name;

	// obtain the bindings for view_name
	Connection con(*context.db);
	auto v = con.Query("select sql_string from _duckdb_ivm_views where view_name = '" + view_name + "';");
	if (v->HasError()) {
		throw InternalException("Error while querying view definition");
	}
	string view_query = v->GetValue(0, 0).ToString();

	// generate column bindings for the view definition
	// we could try and avoid this, but we need to know the column names
	// this is the plan of the view which will be fed to the optimizer rules
	Parser parser;
	parser.ParseQuery(view_query);
	auto statement = parser.statements[0].get();
	Planner planner(context);
	planner.CreatePlan(statement->Copy());

	// create result set using column bindings returned by the planner
	auto result = make_uniq<DoIVMFunctionData>();
	for (size_t i = 0; i < planner.names.size(); i++) {
		return_types.emplace_back(planner.types[i]);
		names.emplace_back(planner.names[i]);
	}

	// add the multiplicity column
	return_types.emplace_back(LogicalTypeId::BOOLEAN);
	names.emplace_back("_duckdb_ivm_multiplicity");

	return std::move(result);
}

static void DoIVMBenchmarkFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = dynamic_cast<DoIVMBenchmarkData &>(*data_p.global_state);
	if (data.offset >= 1) {
		// finished returning values
		return;
	}
	return;
}

static void DoIVMDemoFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = dynamic_cast<DoIVMDemoData &>(*data_p.global_state);
	if (data.offset >= 1) {
		// finished returning values
		return;
	}
	return;
}

static void LoadInternal(ExtensionLoader &loader) {

	auto &instance = loader.GetDatabaseInstance();

	// add a parser extension
	auto &db_config = duckdb::DBConfig::GetConfig(instance);
	db_config.AddExtensionOption("ivm_files_path", "path for compiled files", LogicalType::VARCHAR);
	db_config.AddExtensionOption("ivm_system", "database for cross-system openivm", LogicalType::VARCHAR);
	db_config.AddExtensionOption("ivm_catalog_name", "catalog name", LogicalType::VARCHAR);
	db_config.AddExtensionOption("ivm_schema_name", "schema name", LogicalType::VARCHAR);
	db_config.AddExtensionOption("execute", "whether to execute queries", LogicalType::BOOLEAN);
	db_config.AddExtensionOption("ivm_done", "whether the query has been parsed", LogicalType::BOOLEAN);
	// the "execute" flag is only for benchmarking purposes
	// the logic is: if the database is in memory, there is no point in executing IVM queries
	// however, for my benchmarks it's easier to run every query separately (to measure runtime)
	// so we use this flag to force/avoid execution

	Connection con(instance);
	auto ivm_parser = duckdb::IVMParserExtension();

	auto ivm_rewrite_rule = duckdb::IVMRewriteRule();
	auto ivm_insert_rule = duckdb::IVMInsertRule();

	db_config.parser_extensions.push_back(ivm_parser);
	db_config.optimizer_extensions.push_back(ivm_rewrite_rule);
	db_config.optimizer_extensions.push_back(ivm_insert_rule);

	TableFunction ivm_func("DoIVM", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, DoIVMFunction,
	                       DoIVMBind, DoIVMInit);

	TableFunction ivm_benchmark_groups_func(
	    "IVMBenchmark", {LogicalType::DOUBLE, LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER},
	    DoIVMBenchmarkFunction, DoIVMBenchmarkBind, DoIVMBenchmarkInit);

	TableFunction ivm_benchmark_lineitem_func("IVMBenchmark", {LogicalType::DOUBLE, LogicalType::DOUBLE},
	                                          DoIVMBenchmarkFunction, DoIVMBenchmarkBind, DoIVMBenchmarkInit);

	TableFunction ivm_demo_func(
	    "IVMDemo", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    DoIVMDemoFunction, DoIVMDemoBind, DoIVMDemoInit);

	con.BeginTransaction();
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	ivm_func.bind_replace = reinterpret_cast<table_function_bind_replace_t>(DoIVM);
	ivm_func.name = "DoIVM";
	ivm_func.named_parameters["view_catalog_name"];
	ivm_func.named_parameters["view_schema_name"];
	ivm_func.named_parameters["view_name"];
	CreateTableFunctionInfo ivm_func_info(ivm_func);
	catalog.CreateTableFunction(*con.context, &ivm_func_info);
	con.Commit();

	con.BeginTransaction();
	con.Query("set execute=true");
	con.Commit();

	con.BeginTransaction();
	ivm_benchmark_groups_func.bind_replace = reinterpret_cast<table_function_bind_replace_t>(DoIVMBenchmark);
	ivm_benchmark_groups_func.name = "ivm_benchmark_groups";
	ivm_benchmark_groups_func.named_parameters["scale_factor"];
	ivm_benchmark_groups_func.named_parameters["insertions"];
	ivm_benchmark_groups_func.named_parameters["deletes"];
	ivm_benchmark_groups_func.named_parameters["updates"];
	CreateTableFunctionInfo ivm_benchmark_groups_func_info(ivm_benchmark_groups_func);
	catalog.CreateTableFunction(*con.context, &ivm_benchmark_groups_func_info);
	con.Commit();

	con.BeginTransaction();
	ivm_benchmark_lineitem_func.bind_replace = reinterpret_cast<table_function_bind_replace_t>(DoIVMBenchmark);
	ivm_benchmark_lineitem_func.name = "ivm_benchmark_lineitem";
	ivm_benchmark_lineitem_func.named_parameters["scale_factor"];
	ivm_benchmark_lineitem_func.named_parameters["new_scale_factor"];
	CreateTableFunctionInfo ivm_benchmark_lineitem_func_info(ivm_benchmark_lineitem_func);
	catalog.CreateTableFunction(*con.context, &ivm_benchmark_lineitem_func_info);
	con.Commit();

	con.BeginTransaction();
	ivm_demo_func.bind_replace = reinterpret_cast<table_function_bind_replace_t>(DoIVMDemo);
	ivm_demo_func.name = "ivm_demo";
	ivm_demo_func.named_parameters["catalog_name"];
	ivm_demo_func.named_parameters["schema_name"];
	ivm_demo_func.named_parameters["database_name"];
	ivm_demo_func.named_parameters["path"];
	CreateTableFunctionInfo ivm_demo_func_info(ivm_demo_func);
	catalog.CreateTableFunction(*con.context, &ivm_demo_func_info);
	con.Commit();

	// this is called at the database startup and every time a query fails
	// these three functions are the same - they just take different parameters
	// based on whether we want to specify the catalog and schema
	auto ivm_options = PragmaFunction::PragmaCall("ivm_options", UpsertDeltaQueries,
	                                              {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	loader.RegisterFunction(ivm_options);
	auto ivm = PragmaFunction::PragmaCall("ivm", UpsertDeltaQueries, {LogicalType::VARCHAR});
	loader.RegisterFunction(ivm); // default catalog and schema
	// this is when we have two attached databases (so two catalogs and schemas)
	auto ivm_cross_system = PragmaFunction::PragmaCall(
	    "ivm_cross_system", UpsertDeltaQueries,
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	loader.RegisterFunction(ivm_cross_system);
}

void OpenivmExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string OpenivmExtension::Name() {
	return "openivm";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API const char *ivm_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
