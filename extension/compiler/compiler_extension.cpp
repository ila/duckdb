#include "include/compiler_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"

#include <fstream>
#include <regex>

namespace duckdb {
// common functions to parse SQL strings
// used both in the IVM and RDDA extensions

void CompilerExtension::Load(DuckDB &db) {
	// do nothing
}
string CompilerExtension::Name() {
	return "compiler";
}

void CompilerExtension::WriteFile(const string &filename, bool append, const string &compiled_query) {
	std::ofstream file;
	if (append) {
		file.open(filename, std::ios_base::app);
	} else {
		file.open(filename);
	}
	file << compiled_query << '\n';
	file.close();
}

string CompilerExtension::ReadFile(const string& file_path) {
	string content;
	std::ifstream file(file_path);
	if (file.is_open()) {
		string line;
		while (std::getline(file, line)) {
			content += line + "\n";
		}
		file.close();
	}
	return content;
}

string CompilerExtension::ExtractTableName(const string &sql) {
	// todo check if the regex covers all cases
	std::regex table_name_regex(R"(create\s+table\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_]+)(?:\s*\([^)]*\)|\s+as\s+(.*)))");

	std::smatch match;
	if (std::regex_search(sql, match, table_name_regex)) {
		return match[1].str();
	}
	// return an empty string if there's no match
	return "";
}

string CompilerExtension::ExtractViewName(const string &sql) {
	std::regex view_name_regex(R"(create\s+(?:materialized\s+)?view\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_]+)(?:\s*\([^)]*\)|\s+as\s+(.*)))");
	std::smatch match;
	if (std::regex_search(sql, match, view_name_regex)) {
		return match[1].str();
	}
	// return an empty string if there's no match
	return "";
}

string CompilerExtension::EscapeSingleQuotes(const string &input) {
	std::stringstream escaped_stream;
	for (char c : input) {
		if (c == '\'') {
			escaped_stream << "''"; // append two single quotes
		} else {
			escaped_stream << c; // append the character
		}
	}
	return escaped_stream.str();
}

void CompilerExtension::ReplaceMaterializedView(string &query) {
	// replace "view" with "table" in the query string
	query = std::regex_replace(query, std::regex("\\bmaterialized\\s+view\\b"), "table if not exists");
	// remove the last ;
	query = regex_replace(query, std::regex("\\s*;$"), "");
}

string CompilerExtension::ExtractViewQuery(string &query) {
	std::regex rgx_create_view(R"(create\s+table\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_]+)\s+as\s+(.*))");

	std::smatch match;
	string query_string;

	if (std::regex_search(query, match, rgx_create_view)) {
		return match[2].str();
	}

	return "";
}

string CompilerExtension::SQLToLowercase(const string &sql) {
	// convert the SQL string to lowercase
	// this is necessary because the SQL parser is case-sensitive but SQL is not
	// we need to consider WHERE strings which are case-sensitive (thus we cannot use StringUtil::Lower)
	std::stringstream lowercase_stream;
	bool in_string = false;
	for (char c : sql) {
		if (c == '\'') {
			in_string = !in_string;
		}
		if (!in_string) {
			lowercase_stream << (char)tolower(c);
		} else {
			lowercase_stream << c;
		}
	}
	return lowercase_stream.str();
}

void CompilerExtension::ReplaceCount(string& query) {
	std::regex pattern("(count\\((\\*|\\w+)\\))(?![^()]*\\bas\\b)", std::regex_constants::icase);
	query = std::regex_replace(query, pattern, "count($2) as count_$2");

	// if count(*) is replaced, change it to count_star
	query = std::regex_replace(query, std::regex("count_\\*"), "count_star");
}

void CompilerExtension::ReplaceSum(string& query) {
	std::regex pattern("(sum\\((\\w+)\\))(?![^()]*\\bas\\b)", std::regex_constants::icase);
	query = std::regex_replace(query, pattern, "sum($2) as sum_$2");
}

} // namespace duckdb
