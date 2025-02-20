#include "rdda_parser.hpp"

#include "../../tools/shell/include/shell_state.hpp"
#include "../compiler/include/compiler_extension.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"

#include <data_representation.hpp>
#include <iostream>
#include <rdda/rdda_helpers.hpp>
#include <rdda/rdda_parse_table.hpp>
#include <rdda/rdda_parse_view.hpp>
#include <rdda_parser_helper.hpp>
#include <stack>

namespace duckdb {

string RDDAParserExtension::path;
string RDDAParserExtension::db;

string GetCurrentDatabaseName(Connection &con) {
	auto db_name = con.Query("select current_database();");
	if (db_name->HasError()) {
		throw ParserException("Error while getting database name: " + db_name->GetError());
	}
	return db_name->GetValue(0, 0).ToString();
}

void AttachParserDatabase(Connection &con) {
	auto r = con.Query("attach if not exists 'rdda_parser_internal.db' as rdda_parser_internal;");
	if (r->HasError()) {
		throw ParserException("Error while attaching to rdda_parser_internal.db: " + r->GetError());
	}
	r = con.Query("use rdda_parser_internal");
	if (r->HasError()) {
		throw ParserException("Error while switching to rdda_parser_internal.db: " + r->GetError());
	}
}

void SwitchBackToDatabase(Connection &con, const string &db_name) {
	auto r = con.Query("use " + db_name);
	if (r->HasError()) {
		throw ParserException("Error while switching back to the original database: " + r->GetError());
	}
}

void DetachParserDatabase(Connection &con) {
	auto r = con.Query("detach rdda_parser_internal");
	if (r->HasError()) {
		throw ParserException("Error while detaching from parser db: " + r->GetError());
	}
}

void ExecuteAndWriteQueries(Connection &con, const string &queries, const string &file_path, bool append) {
	if (!queries.empty()) {
		CompilerExtension::WriteFile(file_path, append, queries);
		auto r = con.Query(queries);
		if (r->HasError()) {
			throw ParserException("Error while executing compiled queries: " + r->GetError());
		}
	}
}

void WriteQueries(Connection &con, const string &queries, const string &file_path, bool append) {
	if (!queries.empty()) {
		CompilerExtension::WriteFile(file_path, append, queries);
	}
}

void ParseExecuteQuery(Connection &con, const string &query) {
	auto r = con.Query(query);
	if (r->HasError()) {
		throw ParserException("Error while executing parser queries: " + r->GetError());
	}
}

string ConstructTable(Connection &con, string view_name, bool view) {
	// we make the respective centralized view
	auto table_info = con.TableInfo(view_name);
	if (view) {
		view_name = "rdda_centralized_view_" + view_name;
	} else {
		view_name = "rdda_centralized_table_" + view_name;
	}
	string centralized_table_definition = "create table " + view_name + " (";
	for (auto &column : table_info->columns) {
		centralized_table_definition += column.GetName() + " " + StringUtil::Lower(column.GetType().ToString()) + ", ";
	}
	if (view) {
		centralized_table_definition += "generation timestamptz, arrival timestamptz, rdda_window int, client_id int, action tinyint);\n";
	} else {
		// remove the last comma and space
		centralized_table_definition += "rdda_window int, client_id int);\n";
	}
	return centralized_table_definition;
}

ParserExtensionParseResult RDDAParserExtension::RDDAParseFunction(ParserExtensionInfo *info, const string &query) {
	// very rudimentary parser trying to find RDDA statements
	// the query is parsed twice, so we expect that any SQL mistakes are caught in the second iteration
	// firstly, we try to understand whether this is a SELECT/ALTER/CREATE/DROP/... expression
	auto query_lower = StringUtil::Lower(StringUtil::Replace(query, "\n", " "));
	StringUtil::Trim(query_lower);
	CompilerExtension::RemoveRedundantWhitespaces(query_lower);
	if (query_lower.back() != ';' && query_lower.substr(query_lower.size() - 2) != " ;") {
		query_lower += ";";
	}
	query_lower += "\n";
	auto scope = ParseScope(query_lower);

	if (query_lower.substr(0, 6) == "create") {
		if (scope != TableScope::null) {
			return ParserExtensionParseResult(
		make_uniq_base<ParserExtensionParseData, RDDAParseData>(query_lower, scope));
		}
	}
	return ParserExtensionParseResult();

}

ParserExtensionPlanResult RDDAParserExtension::RDDAPlanFunction(ParserExtensionInfo *info, ClientContext &context,
															  unique_ptr<ParserExtensionParseData> parse_data) {

	auto query = dynamic_cast<RDDAParseData*>(parse_data.get())->query;
	auto scope = dynamic_cast<RDDAParseData*>(parse_data.get())->scope;
	// after the parser, the query string reaches this point

	// each instruction set gets saved to a file, for portability
	string centralized_queries = "";
	string decentralized_queries = "";
	string secure_queries = "";

	string parser_db_name = path + "rdda_parser_internal.db";

	// creating a separate schema such that we can check for syntax errors
	Connection con(*context.db);
	// we get the database name
	auto db_name = GetCurrentDatabaseName(con);
	// we attach to the parser database
	AttachParserDatabase(con);

	if (query.substr(0, 6) == "create") {
		// this is a CREATE statement
		// splitting the statement to extract column names and constraints
		// the table name is the last string that happens before the first parenthesis

		// check if this is a CREATE table or view
		if (query.substr(0, 12) == "create table") {
			// remove RDDA constraints from the string
			// we assume that constraints can also be empty
			unordered_map<string, constraints> constraints;
			if (scope == TableScope::decentralized) {
				constraints = ParseCreateTable(query);
			}

			// query is clean now, let's try and feed it back to the parser
			ParseExecuteQuery(con, query); // todo rollback if error

			auto table_name = CompilerExtension::ExtractTableName(query);

			if (scope == TableScope::decentralized) {
				con.BeginTransaction();
				Parser parser;
				parser.ParseQuery(query);
				auto statement = parser.statements[0].get();
				Planner planner(*con.context);
				planner.CreatePlan(statement->Copy());

				CheckConstraints(*planner.plan, constraints); // we need to do this before writing to files

				// now we add the table in our RDDA catalog (name, scope, query, is_view)
				auto table_string = "insert into rdda_tables values('" + table_name + "', " +
									to_string(static_cast<int32_t>(scope)) + ", NULL, 0);\n";
				centralized_queries += table_string;

				decentralized_queries += query;
				for (auto &constraint : constraints) {
					auto constraint_string = "insert into rdda_table_constraints values ('" + table_name + "', '" +
					                         constraint.first + "', " + to_string(constraint.second.randomized) +
					                         ", " + to_string(constraint.second.sensitive) + ");\n";
					centralized_queries += constraint_string;
				}
				con.Rollback();
			} else {
				if (scope == TableScope::replicated) {
					centralized_queries += query;
					decentralized_queries += query;
					// now we add the table in our RDDA catalog (name, scope, query, is_view)
					auto table_string = "insert into rdda_tables values('" + table_name + "', " +
										to_string(static_cast<int32_t>(scope)) + ", NULL, 0);\n";
					centralized_queries += table_string;
				} else {
					// centralized table
					centralized_queries += query;
					// now we add the table in our RDDA catalog (name, scope, query, is_view)
					auto table_string = "insert into rdda_tables values('" + table_name + "', " +
										to_string(static_cast<int32_t>(scope)) + ", NULL, 0);\n";
					centralized_queries += table_string;
				}
			}

		} else if (query.substr(0, 24) == "create materialized view") {

			if (scope == TableScope::centralized || scope == TableScope::replicated) {
				// if the table is decentralized, we need to check the constraints before parsing
				ParseExecuteQuery(con, query);
			}
			// adding the view to the system tables
			auto view_name = CompilerExtension::ExtractViewName(query);
			auto view_query = CompilerExtension::ExtractViewQuery(query);
			// rdda_centralized_view_ is a reserved name
			if (view_name.substr(0, 23) == "rdda_centralized_view_") {
				throw ParserException("Centralized views cannot start with rdda_centralized_view_");
			}

			if (scope == TableScope::centralized) {
				auto view_string = "insert into rdda_tables values('" + view_name + "', " + to_string(static_cast<int32_t>(scope)) + ", '" + CompilerExtension::EscapeSingleQuotes(view_query) + "', 1);\n";
				centralized_queries += query;
				centralized_queries += view_string;
			} else if (scope == TableScope::decentralized) {
				// here the query should call the openivm parser to parse "materialized view" statements
				// however our schema does not exist, so the parser will emit error
				// to circumvent this, we parse the create table statement
				auto view_constraints = ParseCreateView(query);
				view_query = CompilerExtension::ExtractViewQuery(query); // reinitializing because of constraints
				auto create_table_query = "create table " + view_name + " as " + view_query;
				ParseExecuteQuery(con, create_table_query);
				auto view_constraint_string = "insert into rdda_view_constraints values('" + view_name + "', " +
				                              to_string(view_constraints.window) + ", " +
				                              to_string(view_constraints.ttl) + ", " +
				                              to_string(view_constraints.refresh) + ", " +
				                              to_string(view_constraints.min_agg) + ");\n";
				decentralized_queries += query;
				auto view_string = "insert into rdda_tables values('" + view_name + "', " + to_string(static_cast<int32_t>(scope)) + ", '" + CompilerExtension::EscapeSingleQuotes(view_query) + "', 1);\n";
				centralized_queries += view_string;
				secure_queries += ConstructTable(con, view_name, true);
				view_string = "insert into rdda_tables values('rdda_centralized_view_" + view_name + "', " + to_string(static_cast<int32_t>(TableScope::centralized)) + ", NULL , 1);\n";
				centralized_queries += view_string;
				// we also need to create the respective centralized view
				centralized_queries += ConstructTable(con, view_name, false);
				view_string = "insert into rdda_tables values('rdda_centralized_table_" + view_name + "', " + to_string(static_cast<int32_t>(TableScope::centralized)) + ", NULL , 0);\n";
				centralized_queries += view_string;
				centralized_queries += view_constraint_string;
				auto window_string = "insert into rdda_current_window values('rdda_centralized_table_" + view_name + "', 0);\n";
				centralized_queries += window_string;
			} else {
				// replicated
				auto view_string = "insert into rdda_tables values('" + view_name + "', " + to_string(static_cast<int32_t>(scope)) + ", '" + CompilerExtension::EscapeSingleQuotes(view_query) + "', 1);\n";
				centralized_queries += view_string;
				centralized_queries += query;
				decentralized_queries += query;
			}
		}
	} else if (query.substr(0, 7) == "select") {
		// this is a SELECT statement
		// auto option = ParseSelectQuery(query);
		// todo
	} else if (query.substr(0, 7) == "insert") {
		// this is a INSERT statement
		// todo
	} else if (query.substr(0, 7) == "update") {
		// this is an UPDATE statement
		// todo
	} else if (query.substr(0, 7) == "delete") {
		// this is a DELETE statement
		// todo
	} else if (query.substr(0, 5) == "drop") {
		// this is a DROP statement
		// todo
	} else if (query.substr(0, 11) == "alter table") {
		// this is an ALTER statement
		// todo
	}

	SwitchBackToDatabase(con, db_name);
	DetachParserDatabase(con);

	ExecuteAndWriteQueries(con, centralized_queries, path + "centralized_queries.sql", false);
	WriteQueries(con, decentralized_queries, path + "decentralized_queries.sql", true);
	// todo make the location of secure queries dynamic
	ExecuteAndWriteQueries(con, secure_queries, path + "secure_queries.sql", false);

	ParserExtensionPlanResult result;// register a function with a string as parameter and pass it here (in place of true)
	result.function = RDDAFunction();
	result.parameters.push_back(true); // this could be true or false if we add exception handling
	result.modified_databases = {};
	result.requires_valid_transaction = false;
	result.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

BoundStatement RDDABind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement) {
	return BoundStatement();
}

std::string RDDAParserExtension::Name() {
	return "rdda_parser_extension";
}
}; // namespace duckdb
