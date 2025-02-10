#include "rdda_parser.hpp"

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

#include <iostream>
#include <rdda/rdda_helpers.hpp>
#include <rdda/rdda_parse_table.hpp>
#include <rdda/rdda_parse_view.hpp>
#include <rdda_parser_helper.hpp>
#include <stack>

namespace duckdb {

string RDDAParserExtension::path;
string RDDAParserExtension::db;

ParserExtensionParseResult RDDAParserExtension::RDDAParseFunction(ParserExtensionInfo *info, const string &query) {
	// very rudimentary parser trying to find RDDA statements
	// the query is parsed twice, so we expect that any SQL mistakes are caught in the second iteration
	// firstly, we try to understand whether this is a SELECT/ALTER/CREATE/DROP/... expression

	return ParserExtensionParseResult(
		make_uniq_base<ParserExtensionParseData, RDDAParseData>(query));
}

ParserExtensionPlanResult RDDAParserExtension::RDDAPlanFunction(ParserExtensionInfo *info, ClientContext &context,
															  unique_ptr<ParserExtensionParseData> parse_data) {

	// todo - monday: for some reason "Contracts" is not created in test.db
	// and the metadata tables are created also in rdda_parser_internal.db
	// fix this + refactor

	auto query = dynamic_cast<RDDAParseData*>(parse_data.get())->query;
	// after the parser, the query string reaches this point
	auto query_lower = StringUtil::Lower(StringUtil::Replace(query, "\n", " "));
	StringUtil::Trim(query_lower);

	// each instruction set gets saved to a file, for portability
	string centralized_queries = "";
	string decentralized_queries = "";
	string secure_queries = "";

	string parser_db_name = path + "rdda_parser_internal.db";

	// creating a separate schema such that we can check for syntax errors
	Connection con(*context.db);
	// we get the database name
	auto db_name = con.Query("select current_database();");
	if (db_name->HasError()) {
		throw ParserException("Error while getting database name: ", db_name->GetError());
	}
	auto db_name_str = db_name->GetValue(0, 0).ToString();
	// we attach to the parser database
	auto r = con.Query("attach if not exists 'rdda_parser_internal.db' as rdda_parser_internal;");
	if (r->HasError()) {
		throw ParserException("Error while attaching to rdda_parser_internal.db: ", r->GetError());
	}
	con.Query("use rdda_parser_internal");

	if (query_lower.substr(0, 6) == "create") {
		// this is a CREATE statement
		// splitting the statement to extract column names and constraints
		// the table name is the last string that happens before the first parenthesis
		auto scope = ParseScope(query_lower);
		CompilerExtension::RemoveRedundantWhitespaces(query_lower);
		query_lower += ";\n";

		// check if this is a CREATE table or view
		if (query_lower.substr(0, 12) == "create table") {
			// remove RDDA constraints from the string
			// we assume that constraints can also be empty
			unordered_map<string, constraints> constraints;
			if (scope == TableScope::decentralized) {
				constraints = ParseCreateTable(query_lower);
			}

			// query is clean now, let's try and feed it back to the parser
			auto result = con.Query(query_lower);
			if (result->HasError()) {
				throw ParserException("Error while parsing query: " + result->GetError());
			}

			auto table_name = CompilerExtension::ExtractTableName(query_lower);

			if (scope == TableScope::decentralized) {
				con.BeginTransaction();
				Parser parser;
				parser.ParseQuery(query_lower);
				auto statement = parser.statements[0].get();
				Planner planner(*con.context);
				planner.CreatePlan(statement->Copy());

				CheckConstraints(*planner.plan, constraints); // we need to do this before writing to files

				// now we add the table in our RDDA catalog (name, scope, query, is_view)
				auto table_string = "insert into rdda_tables values('" + table_name + "', " +
									std::to_string(static_cast<int32_t>(scope)) + ", NULL, 0);\n";
				centralized_queries += table_string;

				decentralized_queries += query_lower;
				for (auto &constraint : constraints) {
					auto constraint_string = "insert into rdda_table_constraints values ('" + table_name + "', '" +
					                         constraint.first + "', " + std::to_string(constraint.second.randomized) +
					                         ", " + std::to_string(constraint.second.sensitive) + ", " +
					                         std::to_string(constraint.second.minimum_aggregation) + ");\n";
					centralized_queries += constraint_string;
				}
				con.Rollback();
			} else {
				if (scope == TableScope::replicated) {
					centralized_queries += query_lower;
					decentralized_queries += query_lower;
					// now we add the table in our RDDA catalog (name, scope, query, is_view)
					auto table_string = "insert into rdda_tables values('" + table_name + "', " +
										std::to_string(static_cast<int32_t>(scope)) + ", NULL, 0);\n";
					centralized_queries += table_string;
				} else {
					// centralized table
					centralized_queries += query_lower;
					// now we add the table in our RDDA catalog (name, scope, query, is_view)
					auto table_string = "insert into rdda_tables values('" + table_name + "', " +
										std::to_string(static_cast<int32_t>(scope)) + ", NULL, 0);\n";
					centralized_queries += table_string;
				}
			}

		} else if (query_lower.substr(0, 24) == "create materialized view") {

			auto result = con.Query(query_lower);
			if (result->HasError()) {
				throw ParserException("Error while parsing query: " + result->GetError());
			}
			// adding the view to the system tables
			auto view_name = CompilerExtension::ExtractTableName(query_lower);
			auto view_query = CompilerExtension::ExtractViewQuery(query_lower);
			// rdda_centralized_view_ is a reserved name
			if (view_name.substr(0, 23) == "rdda_centralized_view_") {
				throw ParserException("Centralized views cannot start with rdda_centralized_view_");
			}
			auto view_string = "insert into rdda_tables values(" + view_name + ", " + view_query + ", 1);\n";
			auto query_string = "insert into rdda_queries values(" + view_name + ", " + view_query + ");\n";

			if (scope == TableScope::centralized) {
				centralized_queries += query_lower;
				centralized_queries += view_string;
				centralized_queries += query_string;
			} else if (scope == TableScope::decentralized) {
				auto view_constraints = ParseCreateView(query_lower);
				auto view_constraint_string = "insert into rdda_view_constraints values(" + view_name + ", " +
				                              std::to_string(view_constraints.window) + ", " +
				                              std::to_string(view_constraints.ttl) + ", " +
				                              std::to_string(view_constraints.refresh) + ");\n";
				decentralized_queries += query_lower;
				centralized_queries += view_string;
				centralized_queries += query_string;
				// now we also make the respective centralized view
				string centralized_table_name = "rdda_centralized_view_" + view_name;
				secure_queries += "create table " + centralized_table_name + " as " + view_query + ";\n";
				// alter table: we add generation timestamp, collection timestamp, window and client id
				secure_queries += "alter table " + centralized_table_name +
				                       " add column generation timestamp, add column arrival timestamp, add column window int, add column client_id int;\n";
			} else {
				// replicated
				centralized_queries += view_string;
				centralized_queries += query_string;
				centralized_queries += query_lower;
				decentralized_queries += query_lower;
			}
		}
	} else if (query_lower.substr(0, 7) == "select") {
		// this is a SELECT statement
		// auto option = ParseSelectQuery(query_lower);
		// todo
	} else if (query_lower.substr(0, 7) == "insert") {
		// this is a INSERT statement
		// todo
	} else if (query_lower.substr(0, 7) == "update") {
		// this is an UPDATE statement
		// todo
	} else if (query_lower.substr(0, 7) == "delete") {
		// this is a DELETE statement
		// todo
	} else if (query_lower.substr(0, 5) == "drop") {
		// this is a DROP statement
		// todo
	} else if (query_lower.substr(0, 11) == "alter table") {
		// this is an ALTER statement
		// todo
	}

	r = con.Query("use " + db_name_str);
	if (r->HasError()) {
		throw ParserException("Error while switching back to the original database: " + r->GetError());
	}
	r = con.Query("detach rdda_parser_internal");
	if (r->HasError()) {
		throw ParserException("Error while detaching from parser db: " + r->GetError());
	}

	string path_centralized = path + "centralized_queries.sql";
	string path_decentralized = path + "decentralized_queries.sql";
	string path_secure = path + "secure_queries.sql";

	// writing the newly parsed SQL commands
	if (!centralized_queries.empty()) {
		CompilerExtension::WriteFile(path_centralized, false, centralized_queries);
		r = con.Query(centralized_queries);
		if (r->HasError()) {
			throw ParserException("Error while executing centralized queries: " + r->GetError());
		}
	}
	if (!decentralized_queries.empty()) {
		CompilerExtension::WriteFile(path_decentralized, false, decentralized_queries);
	}
	if (!secure_queries.empty()) {
		CompilerExtension::WriteFile(path_secure, false, secure_queries);
	}

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
