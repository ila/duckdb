#include "../../compiler/include/rdda/rdda_parse_select.hpp"
#include "../../compiler/include/rdda/rdda_parse_table.hpp"
#include "../../compiler/include/rdda/rdda_parse_view.hpp"
#include "duckdb.hpp"
// #include "common.hpp"

#ifndef DUCKDB_RDDA_PARSER_HPP
#define DUCKDB_RDDA_PARSER_HPP

namespace duckdb {

//===--------------------------------------------------------------------===//
// Parser extension
//===--------------------------------------------------------------------===//
struct RDDAExtensionData : public ParserExtensionParseData {

	RDDAExtensionData() {
	}

	duckdb::unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq<RDDAExtensionData>();
	}
};

class RDDAParserExtension : public ParserExtension {
public:
	RDDAParserExtension() {
		parse_function = RDDAParseFunction;
		// plan_function = RDDAPlanFunction;
	}

	static ParserExtensionParseResult RDDAParseFunction(ParserExtensionInfo *info, const string &query);

	/*
	static ParserExtensionPlanResult RDDAPlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                               duckdb::unique_ptr<ParserExtensionParseData> parse_data) {
	return ParserExtensionPlanResult();
	} */

	std::string Name();
	static std::string path;
	static std::string db;
};

} // namespace duckdb

#endif // DUCKDB_RDDA_PARSER_HPP
