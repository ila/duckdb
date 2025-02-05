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

	// auto query_lower = RDDALowerCase(StringUtil::Replace(query, "\n", ""));
	auto query_lower = StringUtil::Lower(StringUtil::Replace(query, "\n", ""));
	StringUtil::Trim(query_lower);
	query_lower += "\n";

	// each instruction set gets saved to a file, for portability
	string parser_query; // the SQL-compliant query to be fed to the parser
	string centralized_queries = "";
	string decentralized_queries = "";
	string secure_queries = "";

	// reading config from settings table
	string db_name = path + db;
	string parser_db_name = path + "parser.db";
	DuckDB settings_db(db_name);
	Connection settings_con(settings_db);

	// creating a backup connection such that we can check for syntax errors
	// this stores all the definitions in a file, such that even at a future moment we have a permanent snapshot
	DuckDB parser_db(parser_db_name);
	Connection parser_con(parser_db);

	if (query_lower.substr(0, 6) == "create") {
		// this is a CREATE statement
		// splitting the statement to extract column names and constraints
		// the table name is the last string that happens before the first parenthesis
		auto scope = ParseScope(query_lower);

		// check if this is a CREATE table or view
		if (query_lower.substr(0, 12) == "create table") {
			// remove RDDA constraints from the string
			// we assume that constraints can also be empty
			unordered_map<string, constraints> constraints;
			if (scope == TableScope::decentralized) {
				constraints = ParseCreateTable(query_lower);
			}

			// query is clean now, let's try and feed it back to the parser
			std::cout << "parsing: " << query_lower << "\n";
			auto result = parser_con.Query(query_lower);
			if (result->HasError()) {
				throw ParserException("Error while parsing query: " + result->GetError());
			}
			auto table_name = CompilerExtension::ExtractTableName(query_lower);
			if (scope == TableScope::decentralized) {
				CheckConstraints(parser_con, query_lower, constraints);
				decentralized_queries += query_lower;
				for (auto &constraint : constraints) {
					// we assume that the system table already exists
					auto constraint_string = "insert into rdda_table_constraints values ('" + table_name + "', '" +
					                         constraint.first + "', " + std::to_string(constraint.second.randomized) +
					                         ", " + std::to_string(constraint.second.sensitive) + ", " +
					                         std::to_string(constraint.second.minimum_aggregation) + ");\n";
					centralized_queries += constraint_string;
				}
			} else {
				if (scope == TableScope::replicated) {
					centralized_queries += query_lower;
					decentralized_queries += query_lower;
				} else {
					// centralized table
					centralized_queries += query_lower;
				}
			}
			// now we add the table in our RDDA catalog (name, scope, query)
			auto table_string = "insert into rdda_tables values(" + table_name + ", " +
			                    std::to_string(static_cast<int32_t>(scope)) + ", NULL);\n";
			centralized_queries += table_string;

		} else if (query_lower.substr(0, 24) == "create materialized view") {

			auto result = parser_con.Query(query_lower);
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
			auto view_string = "insert into rdda_views values(" + view_name + ", " + view_query + ");\n";
			centralized_queries += view_string;
			auto query_string = "insert into rdda_queries values(" + view_name + ", " + view_query + ");\n";
			centralized_queries += query_string;

			if (scope == TableScope::centralized) {
				centralized_queries += query_lower;
			} else if (scope == TableScope::decentralized) {
				auto view_constraints = ParseCreateView(query_lower);
				auto view_constraint_string = "insert into rdda_view_constraints values(" + view_name + ", " +
				                              std::to_string(view_constraints.window) + ", " +
				                              std::to_string(view_constraints.ttl) + ", " +
				                              std::to_string(view_constraints.refresh) + ");\n";
				decentralized_queries += query_lower;
				// now we also make the respective centralized view
				string centralized_table_name = "rdda_centralized_view_" + view_name;
				secure_queries += "create table " + centralized_table_name + " as " + view_query + ";\n";
				// alter table: we add generation timestamp, collection timestamp, window and client id
				secure_queries += "alter table " + centralized_table_name +
				                       " add column generation timestamp, add column arrival timestamp, add column window int, add column client_id int;\n";
			} else {
				// replicated
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

	string path_centralized = path + "centralized_queries.sql";
	string path_decentralized = path + "decentralized_queries.sql";
	string path_secure = path + "secure_queries.sql";

	// writing the newly parsed SQL commands
	if (!centralized_queries.empty()) {
		CompilerExtension::WriteFile(centralized_queries, false, path_centralized);
	}
	if (!decentralized_queries.empty()) {
		CompilerExtension::WriteFile(decentralized_queries, false, path_decentralized);
	}
	if (!secure_queries.empty()) {
		CompilerExtension::WriteFile(secure_queries, false, path_secure);
	}

	std::cout << "done!\n" << std::endl;

	// todo fix this
	return ParserExtensionParseResult("Test!");
}

std::string RDDAParserExtension::Name() {
	return "rdda_parser_extension";
}
}; // namespace duckdb
