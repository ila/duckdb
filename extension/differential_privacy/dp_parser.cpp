#include "include/dp_parser.hpp"

#include <regex>
#include <string>

#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

namespace duckdb {
ParserExtensionParseResult DPParserExtension::DPParseFunction(ParserExtensionInfo *info, const string &query) {

	auto query_lower = StringUtil::Replace(query, "\n", " ");

	StringUtil::Trim(query_lower);

	// todo: include more checks, make more robust
	// Check if query is part of extension syntax
	std::regex syntax_pattern(R"(^create\s+view\s+\w+\s+options\s*\(+.+\)\sas)", std::regex_constants::icase);

	if (std::regex_search(query_lower, syntax_pattern)) {
		// todo: include more checks, make more robust
		// Capture options
		std::regex options_pattern(R"(options\s*\(([^]*)\))", std::regex_constants::icase);
		std::string view_query = std::regex_replace(query_lower, options_pattern, "");
		std::smatch match;
		string threshold, column;

		if (std::regex_search(query_lower, match, options_pattern)) {
			string options_part = match[1];

			StringUtil::Trim(options_part);
			std::istringstream iss(options_part);
			std::string key_value_pair;
			std::smatch match;

			// Go over each option based on ',' delimeter
			while (std::getline(iss, key_value_pair, ',')) {
				StringUtil::Trim(key_value_pair);
				std::regex key_value_pattern(R"(\"(.*)\"\s*:\s*(.*))");
				if (std::regex_search(key_value_pair, match, key_value_pattern)) {
					string key = match[1];
					string value = match[2];

					if (key == "threshold") {
						threshold = value;
						//                            printf("treshold key: %s, value: %s", key.c_str(), value.c_str());

					} else if (key == "privacy_unit_columns") {
						column = StringUtil::Replace(value, "\"", "'");
						;
						//                            printf("privacy key: %s, value: %s", key.c_str(), value.c_str());
					} else {
						// todo unsupported option, throw error
					}
				}
			}
		}

		string meta_query = "INSERT INTO __differential_privacy_metadata VALUES (" + column + "," + threshold + ")";

		// send view and options to planner
		return ParserExtensionParseResult(make_uniq<DPParseData>(view_query, meta_query, true));
	}

	// if not syntax of this extension
	return ParserExtensionParseResult();
}

ParserExtensionPlanResult DPParserExtension::DPPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                            unique_ptr<ParserExtensionParseData> parse_data) {
	auto &parse_info = (DPParseData &)*parse_data;

	if (parse_info.plan) {

		Connection con(*context.db.get());

		// todo implement rollback on failure
		con.BeginTransaction();
		con.Query(parse_info.view_query);
		con.Query("CREATE TABLE __differential_privacy_metadata (column_name VARCHAR, threshold FLOAT,   PRIMARY KEY "
		          "(column_name))");
		con.Query(parse_info.meta_query);
		con.Commit();
	}

	ParserExtensionPlanResult result;
	result.function = DPFunction();
	result.parameters.push_back(true);
	result.requires_valid_transaction = false;
	result.return_type = StatementReturnType::QUERY_RESULT;

	return result;
}
} // namespace duckdb
