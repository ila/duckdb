//
// Created by ila on 19-4-23.
//

#include "../include/rdda/rdda_parse_select.hpp"

#include <regex>
#include <string>
#include <vector>

namespace duckdb {

RDDASelectOption ParseSelectQuery(string &query) {
	RDDASelectOption options;

	std::regex select_regex(
	    "\\b(SELECT\\s+.+\\s+FROM\\s+.+)(?:\\s+WHERE\\s+.+)*(?:\\s+GROUP\\s+BY\\s+.+)*(?:\\s+HAVING\\s+.+)*(?:\\s+"
	    "ORDER\\s+BY\\s+.+)*(?:\\s+LIMIT\\s+\\d+)*(?:\\s+OFFSET\\s+\\d+)*(?:\\s+OPTION\\s*\\(([^\\)]+)\\))?",
	    std::regex_constants::icase);
	std::smatch select_match;

	if (regex_search(query, select_match, select_regex)) {
		// remove the OPTION(...) substring from the query string
		query = regex_replace(query, std::regex("\\s+OPTION\\s*\\([^\\)]+\\)\\s*;?$", std::regex_constants::icase), "");

		// parse option keywords
		string option_string = select_match.str(2);
		if (!option_string.empty()) {
			if (regex_search(option_string, std::regex("\\bresponse ratio\\b", std::regex_constants::icase))) {
				string value_string = regex_replace(
				    option_string,
				    std::regex("^.*\\bresponse ratio\\b\\s+(\\d+\\.?\\d*).*$", std::regex_constants::icase), "$1");
				double response_ratio = stod(value_string);
				if (response_ratio < 0 || response_ratio > 1) {
					throw ParserException("Response ratio should be between 0 and 1.");
				}
				options.response_ratio = response_ratio;
			}

			if (regex_search(option_string, std::regex("\\bminimum response\\b", std::regex_constants::icase))) {
				string value_string = regex_replace(
				    option_string,
				    std::regex("^.*\\bminimum response\\b\\s+(\\d+\\.?\\d*).*$", std::regex_constants::icase), "$1");
				double minimum_response = stod(value_string);
				if (minimum_response < 0 || minimum_response > 1) {
					throw ParserException("Minimum response should be between 0 and 1.");
				}
				options.minimum_response = minimum_response;
			}
		}
	}

	return options;
}
} // namespace duckdb
