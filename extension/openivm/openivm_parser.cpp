#include "include/openivm_parser.hpp"

#include "../compiler/include/compiler_extension.hpp"
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

#include <iostream>
#include <stack>

namespace duckdb {

// upsert: one lookup instead of two lookups (custom operator)

ParserExtensionParseResult IVMParserExtension::IVMParseFunction(ParserExtensionInfo *info, const string &query) {
	// very rudimentary parser trying to find IVM statements
	// the query is parsed twice, so we expect that any SQL mistakes are caught in the second iteration
	// this only works with CREATE MATERIALIZED VIEW expressions

	// todo - test with .open rather than db in the CLI
	// test with new lines? (could also be already fixed in newer versions)

	// todo: find a workaround for the newly created MV that doesn't show up in the shell?

	auto query_lower = CompilerExtension::SQLToLowercase(StringUtil::Replace(query, ";", ""));
	StringUtil::Trim(query_lower);

	// remove new lines (\n)
	query_lower.erase(remove(query_lower.begin(), query_lower.end(), '\n'), query_lower.end());

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
	// todo this should work with the duckAST, double check
	CompilerExtension::ReplaceCount(query_lower);
	CompilerExtension::ReplaceSum(query_lower);

	Parser p;
	p.ParseQuery(query_lower);

	return ParserExtensionParseResult(
	    make_uniq_base<ParserExtensionParseData, IVMParseData>(move(p.statements[0]), true));
}

ParserExtensionPlanResult IVMParserExtension::IVMPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                              unique_ptr<ParserExtensionParseData> parse_data) {
	// after the parser, the query string reaches this point

	// todo for demo:
	// finish (test) benchmarking suite
	// implement timestamps
	// delete from delta table where timestamp = min_timestamp
	// update duckdb_ivm_views set last_update = now (before queries are ran, probably)
	// change ivm_upsert to work with (the schema and) table name
	// refactor DuckAST
	// poster

	auto &ivm_parse_data = dynamic_cast<IVMParseData &>(*parse_data);
	auto statement = dynamic_cast<SQLStatement *>(ivm_parse_data.statement.get());

	if (ivm_parse_data.plan) {

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

		Connection con(*context.db.get());

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

		while (!node_stack.empty()) { // depth-first search
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

		// we create the lookup tables for views -> materialized_view_name | sql_string | type | filter | last_update
		// we need to know if the query has a filter in order to check the timestamp of updates
		auto system_table = "create table if not exists _duckdb_ivm_views (view_name varchar primary key, sql_string "
		                    "varchar, type tinyint, filter bool, last_update timestamp);\n";
		// recreate the file - we assume the queries will be executed after the parsing is done
		CompilerExtension::WriteFile(system_tables_path, false, system_table);

		// fundamentally, each table can have multiple materialized views on it
		// but at some point, we need to delete data from the delta tables (every view has been refreshed)
		// however, we need some kind of versioning to do so, using the last_update column

		// todo - schema and catalog of the views and metadata tables?

		// now we insert the details in the openivm view lookup table
		auto ivm_table_insert = "insert or replace into _duckdb_ivm_views values ('" + view_name + "', '" +
		                        CompilerExtension::EscapeSingleQuotes(view_query) + "', " + to_string((int)ivm_type) +
		                        ", " + to_string(found_filter) + ", now());\n";
		CompilerExtension::WriteFile(system_tables_path, true, ivm_table_insert);

		// now we create the table (the view, internally stored as a table)
		auto table = "create table " + view_name + " as " + view_query + ";\n";
		CompilerExtension::WriteFile(compiled_file_path, false, table);

		// we have the table names; let's create the delta tables (to store insertions, deletions, updates)
		// the API does not support consecutive CREATE + ALTER instructions, so we rewrite it as one query
		// we need IF NOT EXISTS - there can be multiple views over the same table

		auto &catalog = Catalog::GetSystemCatalog(context);
		QueryErrorContext error_context = QueryErrorContext();

		for (const auto &table_name : table_names) {

			Value catalog_value;
			Value schema_value;

			// we also need to replicate the constraints of the base tables to the delta tables
			// this is because if an INSERT/UPDATE/DELETE fails on the table, it should also fail on the delta table
			// usually these operations fail because of violated constraints

			// to do so, we simply manipulate the original CREATE string adding the multiplicity and timestamp columns

			// all this stuff is needed to extract the base table from the catalog
			if (catalog_value.IsNull() && !context.db->config.options.database_path.empty()) {
				auto catalog_name =
				    con.Query("select table_catalog from information_schema.tables where table_name = '" + table_name +
				              "';")
				        ->Fetch()
				        ->GetValue(0, 0);
				catalog_value = catalog_name;
			} else if (catalog_value.IsNull()) { // an in-memory database
				catalog_value = Value("memory");
			}

			if (schema_value.IsNull()) {
				schema_value = Value("main"); // default schema
			}

			auto catalog_entry =
			    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, catalog_value.ToString(), schema_value.ToString(),
			                     table_name, OnEntryNotFound::THROW_EXCEPTION, error_context);
			auto table_entry = dynamic_cast<TableCatalogEntry *>(catalog_entry.get());
			auto table_string = table_entry->ToSQL();

			auto delta_table = CompilerExtension::GenerateDeltaTable(table_string);
			CompilerExtension::WriteFile(compiled_file_path, true, delta_table);
		}

		// todo handle the case of replacing column names

		// now we also create a view (for internal use, just to store the SQL query)
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
			for (auto &query : StringUtil::Split(system_queries, '\n')) {
				auto r = con.Query(query);
				if (r->HasError()) {
					throw Exception(ExceptionType::PARSER, "Could not create system tables: " + r->GetError());
				}
			}

			auto queries = duckdb::CompilerExtension::ReadFile(compiled_file_path);

			// we split the queries one by one separated by newline
			// we need to do this because DuckDB won't throw errors with multiple queries in one string
			for (auto &query : StringUtil::Split(queries, '\n')) {
				auto r = con.Query(query);
				if (r->HasError()) {
					throw Exception(ExceptionType::PARSER, "Could not create materialized view: " + r->GetError());
				}
			}

			if (ivm_type == IVMType::AGGREGATE_GROUP) {
				auto index = duckdb::CompilerExtension::ReadFile(index_file_path);
				auto r = con.Query(index);
				if (r->HasError()) {
					throw Exception(ExceptionType::PARSER, "Could not create index: " + r->GetError());
				}
			}
		}
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
	return BoundStatement();
}
}; // namespace duckdb
