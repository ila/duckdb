#include "include/ivm_parser.hpp"

#include "../compiler/include/rdda/rdda_parse_table.hpp"
#include "../compiler/include/rdda/rdda_parse_view.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"
#include "../compiler/include/compiler_extension.hpp"

#include <iostream>
#include <stack>

namespace duckdb {

ParserExtensionParseResult IVMParserExtension::IVMParseFunction(ParserExtensionInfo *info, const string &query) {
	// very rudimentary parser trying to find IVM statements
	// the query is parsed twice, so we expect that any SQL mistakes are caught in the second iteration
	// this only works with CREATE MATERIALIZED VIEW expressions
	auto query_lower = CompilerExtension::SQLToLowercase(StringUtil::Replace(query, ";", ""));
	StringUtil::Trim(query_lower);

	// each instruction set gets saved to a file, for portability
	// the SQL-compliant query to be fed to the parser
	if (!StringUtil::Contains(query_lower, "create materialized view")) {
		return ParserExtensionParseResult();
	}

	// we see a materialized view - but we create a table under the hood
	// first we turn this instruction into a table ddl
	CompilerExtension::ReplaceMaterializedView(query_lower);

	// we do not support aggregate functions without aliases
	// for portability reasons, so we create internal aliases
	CompilerExtension::ReplaceCount(query_lower);
	CompilerExtension::ReplaceSum(query_lower);

	Parser p;
	p.ParseQuery(query_lower);

	return ParserExtensionParseResult(make_uniq_base<ParserExtensionParseData, IVMParseData>(move(p.statements[0])));
}

ParserExtensionPlanResult IVMParserExtension::IVMPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                              unique_ptr<ParserExtensionParseData> parse_data) {

	auto &ivm_parse_data = dynamic_cast<IVMParseData &>(*parse_data);
	auto statement = dynamic_cast<SQLStatement *>(ivm_parse_data.statement.get());

	// we extract the table name
	auto view_name = CompilerExtension::ExtractTableName(statement->query);
	// then we extract the query
	auto view_query = CompilerExtension::ExtractViewQuery(statement->query);

	// now we create the delta table based on the view definition
	// parsing the logical plan
	string db_path;
	if (!context.db->config.options.database_path.empty()) {
		db_path = context.db->GetFileSystem().GetWorkingDirectory();
	} else {
		Value db_path_value;
		context.TryGetCurrentSetting("ivm_files_path", db_path_value);
		db_path = db_path_value.ToString();
	}
	string compiled_file_path = db_path + "/ivm_compiled_queries_" + view_name + ".sql";
	string system_tables_path = db_path + "/ivm_system_tables.sql";
	// we need a separate index file because it is faster to create the index after the materialized view
	auto index_file_path = db_path + "/ivm_index_" + view_name + ".sql";

	// CompilerExtension::WriteFile(compiled_file_path, false, statement->query);

	Connection con(*context.db.get());

	con.BeginTransaction();
	auto table_names = con.GetTableNames(statement->query);

	Planner planner(context);

	planner.CreatePlan(statement->Copy());
	auto plan = move(planner.plan);

	std::stack<LogicalOperator *> node_stack;
	node_stack.push(plan.get());

	bool found_filter = false;
	bool found_aggregation = false;
	bool found_projection = false;
	vector<string> aggregate_columns;

	while (!node_stack.empty()) {
		auto current = node_stack.top();
		node_stack.pop();

		if (current->type == LogicalOperatorType::LOGICAL_FILTER) {
			found_filter = true;
		}

		if (current->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			found_aggregation = true;
			// find the aggregation column(s) in order to create an index
			auto node = dynamic_cast<LogicalAggregate *>(current);
			for (auto &group : node->groups) {
				auto column = dynamic_cast<BoundColumnRefExpression *>(group.get());
				aggregate_columns.emplace_back(column->alias);
			}
		}

		if (current->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			found_projection = true;
		}

		if (!current->children.empty()) {
			// push children onto the stack in reverse order to process them in a depth-first manner
			for (auto it = current->children.rbegin(); it != current->children.rend(); ++it) {
				node_stack.push(it->get());
			}
		}
	}

	IVMType ivm_type;

	if (found_aggregation && !aggregate_columns.empty()) {
		// select stuff, COUNT(*) FROM table [WHERE condition] GROUP BY stuff;
		ivm_type = IVMType::AGGREGATE_GROUP;
	} else if (found_aggregation && aggregate_columns.empty()) {
		// SELECT COUNT(*) FROM table; (without GROUP BY)
		ivm_type = IVMType::SIMPLE_AGGREGATE;
	} else if (found_filter && !found_aggregation) {
		// SELECT stuff FROM table WHERE condition;
		ivm_type = IVMType::SIMPLE_FILTER;
	} else if (found_projection && !found_aggregation) {
		// SELECT stuff FROM table;
		ivm_type = IVMType::SIMPLE_PROJECTION;
	} else {
		throw NotImplementedException("IVM does not support this query type yet");
	}

	con.Rollback();

	// we create the lookup tables for views -> materialized_view_name | sql_string | type | plan
	auto system_table = "create table if not exists _duckdb_ivm_views (view_name varchar primary key, sql_string "
	                    "varchar, type tinyint, plan varchar);\n";
	// recreate the file - we assume the queries will be executed after the parsing is done
	CompilerExtension::WriteFile(system_tables_path, false, system_table);

	// now we insert the details in the ivm view lookup table
	// firstly we need to serialize the plan to a string
	// commenting because the Postgres scanner does not have a serializer yet
	/*
	MemoryStream target;
	BinarySerializer serializer(target);
	serializer.Begin();
	plan->Serialize(serializer);
	serializer.End();
	auto data = target.GetData();
	idx_t len = target.GetPosition();
	string serialized_plan(data, data + len); */

	// do we need insert or replace here? insert or ignore? am I just overthinking?
	// todo this does not work because of special characters
	// else we just store the query and re-plan it each time or store the json
	// auto x = escapeSingleQuotes(serialized_plan);
	// auto test = "create table if not exists test (plan varchar);\n";
	// con.Query(test);
	// auto res = con.Query("insert into test values('" + x + "');\n");

	auto test = "abc";
	auto ivm_table_insert = "insert or replace into _duckdb_ivm_views values ('" + view_name + "', '" +
	                        CompilerExtension::EscapeSingleQuotes(view_query) + "', " + to_string((int)ivm_type) +
	                        ", '" + test + "');\n";
	CompilerExtension::WriteFile(system_tables_path, true, ivm_table_insert);

	// now we create the table (the view, internally stored as a table)
	auto table = "create table if not exists " + view_name + " as " + view_query + ";\n";
	CompilerExtension::WriteFile(compiled_file_path, false, table);

	// we have the table names; let's create the delta tables (to store insertions, deletions, updates)
	// the API does not support consecutive CREATE + ALTER instructions, so we rewrite it as one query
	// CREATE TABLE IF NOT EXISTS delta_table AS SELECT *, TRUE AS _duckdb_ivm_multiplicity FROM my_table LIMIT 0;
	for (const auto &table_name : table_names) {
		// todo schema (add a option like the path)
		// todo also add the view name here (there can be multiple views?)
		// todo exception handling
		auto delta_table = "create table if not exists p.public.delta_" + table_name +
		                   " as select *, true as _duckdb_ivm_multiplicity from " + table_name + " limit 0;\n";
		CompilerExtension::WriteFile(compiled_file_path, true, delta_table);
	}

	// todo handle the case of replacing column names

	// now we also create a view (for internal use, just to store the SQL query)
	// todo - remove this if I manage to make the lookup table work
	auto view = "create or replace view _duckdb_internal_" + view_name + "_ivm as " + view_query + ";\n";
	CompilerExtension::WriteFile(compiled_file_path, true, view);

	// now we create the delta table for the result (to store the IVM algorithm output)
	string delta_view = "create table if not exists delta_" + view_name +
	                    " as select *, true as _duckdb_ivm_multiplicity from " + view_name + " limit 0;\n";
	CompilerExtension::WriteFile(compiled_file_path, true, delta_view);

	// lastly, we need to create an index on the aggregation keys on the view and delta result table
	if (ivm_type == IVMType::AGGREGATE_GROUP) {
		string index_query_view = "create unique index " + view_name + "_ivm_index on " + view_name + "(";
		for (size_t i = 0; i < aggregate_columns.size(); i++) {
			index_query_view += aggregate_columns[i];
			if (i != aggregate_columns.size() - 1) {
				index_query_view += ", ";
			}
		}
		index_query_view += ");\n";
		// writing to file
		CompilerExtension::WriteFile(index_file_path, false, index_query_view);
	}

	string comment = "-- code to propagate operations to the base table goes here\n";
	comment += "-- assuming the changes to be in the delta tables\n";
	CompilerExtension::WriteFile(compiled_file_path, true, comment);

	// we execute the file to apply the changes
	// .read (CLI command) doesn't work with the API, so we read the file first
	// only executing this if the database is not in memory
	if (!context.db->config.options.database_path.empty()) {
		auto system_queries = duckdb::CompilerExtension::ReadFile(system_tables_path);
		auto r1 = con.Query(system_queries);
		auto queries = duckdb::CompilerExtension::ReadFile(compiled_file_path);
		auto r2 = con.Query(queries);
		auto index = duckdb::CompilerExtension::ReadFile(index_file_path);
		con.Query(index);
	}

	ParserExtensionPlanResult result;
	result.function = IVMFunction();
	result.parameters.push_back(true); // this could be true or false if we add exception handling
	result.modified_databases = {};
	result.requires_valid_transaction = false;
	result.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

BoundStatement IVMBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement) {
	printf("In IVM Bind function\n");
	return BoundStatement();
}
}; // namespace duckdb
