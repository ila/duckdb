#include "duckdb.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/extension_util.hpp"
#

#include <utility>

#ifndef DUCKDB_DP_PARSER_HPP
#define DUCKDB_DP_PARSER_HPP

namespace duckdb {

class DPParserExtension : public ParserExtension {
public:
	explicit DPParserExtension() {
		parse_function = DPParseFunction;
		plan_function = DPPlanFunction;
	}

	static ParserExtensionParseResult DPParseFunction(ParserExtensionInfo *info, const string &query);
	static ParserExtensionPlanResult DPPlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                                unique_ptr<ParserExtensionParseData> parse_data);
};

BoundStatement DPBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement);

struct DPOperatorExtension : public OperatorExtension {
	DPOperatorExtension() : OperatorExtension() {
		Bind = DPBind;
	}

	std::string GetName() override {
		return "DP";
	}
};

struct DPParseData : public ParserExtensionParseData {
	DPParseData(string view_query, string meta_query, bool plan)
	    : view_query(view_query), meta_query(meta_query), plan(plan) {
	}

	string view_query;
	string meta_query;
	// the "plan" flag is to avoid the content in the body of the planner to be executed multiple times
	bool plan = false;

	duckdb::unique_ptr<ParserExtensionParseData> Copy() const override {
		// we pass "false" here because if we get here, we already parsed the query
		// DuckDB copies the function data but we don't need to execute the planner function
		return make_uniq<DPParseData>(view_query, meta_query, false);
	}

	string ToString() const override {
		// placeholder to have all virtual functions implemented
		return view_query;
	}
};

class DPState : public ClientContextState {
public:
	explicit DPState(unique_ptr<ParserExtensionParseData> parse_data) : parse_data(std::move(parse_data)) {
	}

	void QueryEnd() override {
		parse_data.reset();
	}

	unique_ptr<ParserExtensionParseData> parse_data;
};

class DPFunction : public TableFunction {
public:
	DPFunction() {
		name = "DP function";
		arguments.push_back(LogicalType::BOOLEAN); // parsing successful
		bind = DPBind;
		init_global = DPInit;
		function = DPFunc;
	}

	struct DPBindData : public TableFunctionData {

		explicit DPBindData(bool result) : result(result) {
		}

		bool result;
	};

	struct DPGlobalData : public GlobalTableFunctionState {
		DPGlobalData() : offset(0) {
		}

		idx_t offset;
	};

	static duckdb::unique_ptr<FunctionData> DPBind(ClientContext &context, TableFunctionBindInput &input,
	                                               vector<LogicalType> &return_types, vector<string> &names) {

		names.emplace_back("DP");
		return_types.emplace_back(LogicalType::BOOLEAN);
		bool result = true;
		return make_uniq<DPBindData>(result);
	}

	static duckdb::unique_ptr<GlobalTableFunctionState> DPInit(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<DPGlobalData>();
	}

	static void DPFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		// placeholder (this needs to return something)
		printf("Inside DPFunc of Table function class\n");
	}
};

} // namespace duckdb

#endif // DUCKDB_DP_PARSER_HPP
