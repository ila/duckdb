#include "../include/rdda/rdda_parse_table.hpp"

#include <iostream>
#include <regex>
#include <string>
#include <vector>

namespace duckdb {

TableScope ParseScope(std::string &query) {
	std::regex scope_regex("\\b(create)\\s+(\\bcentralized\\b|\\bdecentralized\\b|\\breplicated\\b)\\s+(table|"
	                       "materialized\\s+view)\\s+(.*)");
	std::smatch scope_match;
	TableScope scope = TableScope::null;

	if (std::regex_search(query, scope_match, scope_regex)) {
		if (scope_match.size() == 5) {
			if ((scope_match[2].str() == "centralized" &&
			     (scope_match[3].str() == "decentralized" || scope_match[3].str() == "replicated")) ||
			    (scope_match[2].str() == "decentralized" &&
			     (scope_match[3].str() == "centralized" || scope_match[3].str() == "replicated")) ||
			    (scope_match[2].str() == "replicated" &&
			     (scope_match[3].str() == "decentralized" || scope_match[3].str() == "centralized"))) {
				throw ParserException("Cannot specify multiple table scopes");
			}
			if (scope_match[2].str() == "centralized") {
				scope = TableScope::centralized;
			} else if (scope_match[2].str() == "decentralized") {
				scope = TableScope::decentralized;
			} else { // replicated
				scope = TableScope::replicated;
			}
			query = scope_match[1].str() + " " + scope_match[3].str() + " " + scope_match[4].str();
		}
	} else {
		throw ParserException("Object should be centralized, decentralized or replicated!");
	}
	return scope;
}

unordered_map<string, constraints> ParseCreateTable(std::string &query) {

	unordered_map<string, constraints> constraints_table;

	std::regex columns_regex("\\((.*)\\)");
	std::smatch columns_match;
	if (regex_search(query, columns_match, columns_regex)) {
		std::string columns_definition = columns_match[1].str();

		std::regex column_name_regex(R"(\b(\w+)\s)");
		std::regex split_regex(",\\s*");
		std::sregex_token_iterator split_iter(columns_definition.begin(), columns_definition.end(), split_regex, -1);
		std::sregex_token_iterator split_end;

		for (; split_iter != split_end; split_iter++) {
			std::string split = split_iter->str();
			StringUtil::Trim(split);

			// parsing column name
			constraints constraints_column;
			string column_name;
			std::smatch column_name_matches;
			if (regex_search(split, column_name_matches, column_name_regex)) {
				column_name = column_name_matches.str(1);
			}

			// checking for randomized
			if (regex_search(split, std::regex("\\brandomized\\b"))) {
				constraints_column.randomized = true;
			}

			// checking for minimum aggregation
			if (regex_search(split, std::regex("\\bminimum aggregation\\b"))) {
				int minimum_aggregation_value =
				    stoi(regex_replace(split, std::regex("^.*\\bminimum aggregation\\b\\s+(\\d+).*$"), "$1"));
				if (minimum_aggregation_value <= 0) {
					throw ParserException("Invalid minimum aggregation value, must be greater than zero.");
				}
				constraints_column.minimum_aggregation = minimum_aggregation_value;
			}

			if (regex_search(split, std::regex("\\bsensitive\\b"))) {
				constraints_column.sensitive = true;
			}

			StringUtil::Trim(split);
			constraints_table.insert(make_pair(column_name, constraints_column));
		}

		// remove keywords from the query string
		query =
		    regex_replace(query, std::regex("\\bsensitive\\b|\\brandomized\\b|\\bminimum aggregation\\s+\\d+\\b"), "");
	}

	return constraints_table;
}

} // namespace duckdb
