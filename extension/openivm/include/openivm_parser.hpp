#include "duckdb.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "openivm_upsert.hpp"

#include <utility>

#ifndef DUCKDB_IVM_PARSER_HPP
#define DUCKDB_IVM_PARSER_HPP

namespace duckdb {

struct DoIVMFunctionData : TableFunctionData {
	DoIVMFunctionData() {
	}
};

struct DoIVMBenchmarkFunctionData : TableFunctionData {
	DoIVMBenchmarkFunctionData() {
	}
};

struct IVMInfo : ParserExtensionInfo {
	unique_ptr<Connection> db_conn;
	bool performed = false;
	explicit IVMInfo(unique_ptr<Connection> db_conn) : db_conn(std::move(db_conn)) {
	}
};

class IVMParserExtension : public ParserExtension {
public:
	explicit IVMParserExtension() {
		parse_function = IVMParseFunction;
		plan_function = IVMPlanFunction;
	}

	static ParserExtensionParseResult IVMParseFunction(ParserExtensionInfo *info, const string &query);
	static ParserExtensionPlanResult IVMPlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                                 unique_ptr<ParserExtensionParseData> parse_data);
};

BoundStatement IVMBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement);

struct IVMOperatorExtension : public OperatorExtension {
	IVMOperatorExtension() : OperatorExtension() {
		Bind = IVMBind;
	}

	std::string GetName() override {
		return "IVM";
	}
};

struct IVMParseData : ParserExtensionParseData {
	IVMParseData() {
	}

	unique_ptr<SQLStatement> statement;
	// the "plan" flag is to avoid the content in the body of the planner to be executed multiple times
	bool plan = false;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		// we pass "false" here because if we get here, we already parsed the query
		// DuckDB copies the function data, but we don't need to execute the planner function
		return make_uniq_base<ParserExtensionParseData, IVMParseData>(statement->Copy(), false);
	}

	string ToString() const override {
		return statement->ToString();
	}

	explicit IVMParseData(unique_ptr<SQLStatement> statement, bool plan) : statement(std::move(statement)), plan(plan) {
	}


};

class IVMState : public ClientContextState {
public:
	explicit IVMState(unique_ptr<ParserExtensionParseData> parse_data) : parse_data(std::move(parse_data)) {
	}

	void QueryEnd() override {
		parse_data.reset();
	}

	unique_ptr<ParserExtensionParseData> parse_data;
};

class IVMFunction : public TableFunction {
public:
	IVMFunction() {
		name = "IVM function";
		arguments.push_back(LogicalType::BOOLEAN); // parsing successful
		bind = IVMBind;
		init_global = IVMInit;
		function = IVMFunc;
	}

	struct IVMBindData : public TableFunctionData {

		explicit IVMBindData(bool result) : result(result) {
		}

		bool result;
	};

	struct IVMGlobalData : public GlobalTableFunctionState {
		IVMGlobalData() : offset(0) {
		}

		idx_t offset;
	};

	static unique_ptr<FunctionData> IVMBind(ClientContext &context, TableFunctionBindInput &input,
	                                                vector<LogicalType> &return_types, vector<string> &names) {


		names.emplace_back("MATERIALIZED VIEW CREATION");
		return_types.emplace_back(LogicalType::BOOLEAN);
		bool result = false;
		if (IntegerValue::Get(input.inputs[0]) == 1) {
			result = true; // explict creation of the result since the input is an integer value for some reason
		}
		return make_uniq<IVMBindData>(result);
	}

	static unique_ptr<GlobalTableFunctionState> IVMInit(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IVMGlobalData>();
	}

	static void IVMFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		// placeholder (this needs to return something)
		// printf("Inside IVMFunc of Table function class\n");
		auto &bind_data = data_p.bind_data->Cast<IVMBindData>();
		auto &data = dynamic_cast<IVMGlobalData &>(*data_p.global_state);
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

#endif // DUCKDB_IVM_PARSER_HPP
