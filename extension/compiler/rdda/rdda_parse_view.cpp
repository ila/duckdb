#include "../include/rdda/rdda_parse_view.hpp"
#include "../include/compiler_extension.hpp"

#include <regex>
#include <string>

namespace duckdb {

RDDAViewConstraint ParseCreateView(string &query) {

	RDDAViewConstraint constraint;

	std::regex window_regex("\\bwindow\\s+(\\d+)\\b");
	std::smatch window_match;
	if (regex_search(query, window_match, window_regex)) {
		constraint.window = std::stoi(window_match[1].str());
		if (constraint.window <= 0) {
			throw ParserException("Invalid value for WINDOW, must be greater than zero!");
		}
		query = regex_replace(query, window_regex, "");
	}

	std::regex ttl_regex("\\bttl\\s+(\\d+)\\b");
	std::smatch ttl_match;
	if (regex_search(query, ttl_match, ttl_regex)) {
		constraint.ttl = std::stoi(ttl_match[1].str());
		if (constraint.ttl <= 0) {
			throw ParserException("Invalid value for TTL, must be greater than zero!");
		}
		query = regex_replace(query, ttl_regex, "");
	}

	CompilerExtension::ReplaceMaterializedView(query);
	return constraint;
}

string ParseViewTables(string &query) {
	// IN also works with trailing comma

	// define a regex pattern to match the FROM clause
	std::regex from_clause("FROM\\s+([^\\s,]+)(?:\\s+(?:JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN)\\s+([^\\s,]+))?");
	std::vector<std::string> tables;

	// create a regex iterator to iterate over matches
	std::sregex_iterator it(query.begin(), query.end(), from_clause);
	std::sregex_iterator end;

	// iterate over matches and extract the table names
	for (; it != end; ++it) {
		const std::smatch &match = *it;
		tables.push_back(match[1]);
		if (match[2].str().length() > 0) {
			tables.push_back(match[2]);
		}
	}

	// concatenate the matches in a string
	string in;
	for (auto t = tables.begin(); t != tables.end(); ++t) {
		if (t != tables.begin()) {
			in += ", ";
		}
		in += *t;
	}

	return in;
}

} // namespace duckdb
