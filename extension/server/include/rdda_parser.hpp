#include "duckdb.hpp"

#ifndef DUCKDB_RDDA_PARSER_HPP
#define DUCKDB_RDDA_PARSER_HPP

namespace duckdb {

//===--------------------------------------------------------------------===//
// Parser extension
//===--------------------------------------------------------------------===//
struct RDDAParseData : ParserExtensionParseData {

	string query;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		// we pass "false" here because if we get here, we already parsed the query
		// DuckDB copies the function data, but we don't need to execute the planner function
		return make_uniq_base<ParserExtensionParseData, RDDAParseData>(query);
	}

	string ToString() const override {
		return query;
	}

	explicit RDDAParseData(string query) : query(query) {
	}
};

class RDDAParserExtension : public ParserExtension {
public:
	RDDAParserExtension() {
		parse_function = RDDAParseFunction;
		plan_function = RDDAPlanFunction;
	}

	static ParserExtensionParseResult RDDAParseFunction(ParserExtensionInfo *info, const string &query);

	static ParserExtensionPlanResult RDDAPlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                                  duckdb::unique_ptr<ParserExtensionParseData> parse_data);

	string Name();
	static string path;
	static string db;
};

class RDDAFunction : public TableFunction {
public:
	RDDAFunction() {
		name = "RDDA function";
		arguments.push_back(LogicalType::BOOLEAN); // parsing successful
		bind = RDDABind;
		init_global = RDDAInit;
		function = RDDAFunc;
	}

	struct RDDABindData : TableFunctionData {

		explicit RDDABindData(bool result) : result(result) {
		}

		bool result;
	};

	struct RDDAGlobalData : GlobalTableFunctionState {
		RDDAGlobalData() : offset(0) {
		}

		idx_t offset;
	};

	static unique_ptr<FunctionData> RDDABind(ClientContext &context, TableFunctionBindInput &input,
	                                                 vector<LogicalType> &return_types, vector<string> &names) {

		names.emplace_back("RDDA OBJECT CREATION");
		return_types.emplace_back(LogicalType::BOOLEAN);
		bool result = false;
		if (IntegerValue::Get(input.inputs[0]) == 1) {
			result = true; // explict creation of the result since the input is an integer value for some reason
		}
		return make_uniq<RDDABindData>(result);
	}

	static unique_ptr<GlobalTableFunctionState> RDDAInit(ClientContext &context,
	                                                             TableFunctionInitInput &input) {
		return make_uniq<RDDAGlobalData>();
	}

	static void RDDAFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		// placeholder (this needs to return something)
		auto &bind_data = data_p.bind_data->Cast<RDDABindData>();
		auto &data = dynamic_cast<RDDAGlobalData &>(*data_p.global_state);
		if (data.offset >= 1) {
			// finished returning values
			return;
		}
		auto result = Value::BOOLEAN(bind_data.result);
		data.offset++;
		output.SetValue(0, 0, result);
		output.SetCardinality(1);
	}
};

} // namespace duckdb

#endif // DUCKDB_RDDA_PARSER_HPP
