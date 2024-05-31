#include "include/compiler_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "include/logical_plan_to_string.hpp"

#include <fstream>
#include <regex>

namespace duckdb {
// common functions to parse SQL strings
// used both in the IVM and RDDA extensions

// table function definitions
//------------------------------------------------------------------------------
struct LogicalPlanToStringTestData : public GlobalTableFunctionState {
	LogicalPlanToStringTestData() : offset(0) {
	}
	idx_t offset;
};

unique_ptr<GlobalTableFunctionState> LogicalPlanToStringTestInit(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto result = make_uniq<LogicalPlanToStringTestData>();
	return std::move(result);
}

static unique_ptr<TableRef> LogicalPlanToStringTest(ClientContext &context, TableFunctionBindInput &input) {
	return nullptr;
}

struct LogicalPlanToStringTestFunctionData : public TableFunctionData {
	LogicalPlanToStringTestFunctionData() {
	}
};

static duckdb::unique_ptr<FunctionData> LogicalPlanToStringTestBind(ClientContext &context,
                                                                    TableFunctionBindInput &input,
                                                                    vector<LogicalType> &return_types,
                                                                    vector<string> &names) {
	// called when the pragma is executed
	// specifies the output format of the query (columns)
	// display the outputs (do not remove)
	auto query = StringValue::Get(input.inputs[0]);

	input.named_parameters["query"] = query;

	Parser parser;
	parser.ParseQuery(query);
	auto statement = parser.statements[0].get();
	Planner planner(context);
	planner.CreatePlan(statement->Copy());
	planner.plan->Print();

	string planString = LogicalPlanToString(context, planner.plan);
	Printer::Print("String: " + planString);

	// create result set using column bindings returned by the planner
	auto result = make_uniq<LogicalPlanToStringTestFunctionData>();

	return_types.emplace_back(LogicalTypeId::BOOLEAN);
	names.emplace_back("Done");

	return std::move(result);
}

static void LogicalPlanToStringTestFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = dynamic_cast<LogicalPlanToStringTestData &>(*data_p.global_state);
	if (data.offset >= 1) {
		// finished returning values
		return;
	}
	return;
}

//------------------------------------------------------------------------------

static void LoadInternal(DatabaseInstance &instance) {

	// add a compiler extension
	// auto &db_config = duckdb::DBConfig::GetConfig(instance);
	// eventual flags for the compiler extension go here
	// db_config.AddExtensionOption("ivm_files_path", "path for compiled files", LogicalType::VARCHAR);

	Connection con(instance);
	auto compiler = duckdb::CompilerExtension();

	// db_config.parser_extensions.push_back(ivm_parser);
	// db_config.optimizer_extensions.push_back(ivm_rewrite_rule);

	TableFunction lpts_func("LogicalPlanToStringTest", {LogicalType::VARCHAR}, LogicalPlanToStringTestFunction,
	                        LogicalPlanToStringTestBind, LogicalPlanToStringTestInit);

	con.BeginTransaction();
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	lpts_func.bind_replace = reinterpret_cast<table_function_bind_replace_t>(LogicalPlanToStringTest);
	lpts_func.name = "lpts_test";
	lpts_func.named_parameters["query"];
	CreateTableFunctionInfo lpts_func_info(lpts_func);
	catalog.CreateTableFunction(*con.context, &lpts_func_info);
	con.Commit();
}

void CompilerExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
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

string CompilerExtension::ReadFile(const string &file_path) {
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
	std::regex table_name_regex(
	    R"(create\s+table\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_]+)(?:\s*\([^)]*\)|\s+as\s+(.*)))");

	std::smatch match;
	if (std::regex_search(sql, match, table_name_regex)) {
		return match[1].str();
	}
	// return an empty string if there's no match
	return "";
}

string CompilerExtension::ExtractViewName(const string &sql) {
	std::regex view_name_regex(
	    R"(create\s+(?:materialized\s+)?view\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_]+)(?:\s*\([^)]*\)|\s+as\s+(.*)))");
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

string CompilerExtension::GenerateDeltaTable(string &query) {
	// we need to do three things here:
	// 1) replace the table_name (everything after the last dot and before the first "(") with "delta_table_name"
	// 2) add a new bool column multiplicity
	// 3) add a new timestamp column with default now()
	string delta_query = SQLToLowercase(query);
	// remove any \" from the query
	delta_query = std::regex_replace(delta_query, std::regex(R"(\")"), "");
	// extract the primary key(...) string
	string primary_key_string;
	std::regex rgx_primary_key(R"(primary\s+key\s*\([^)]+\))");
	std::smatch match;
	if (std::regex_search(delta_query, match, rgx_primary_key)) {
		primary_key_string = match[0];
	}
	// remove the primary key string from the query
	delta_query = std::regex_replace(delta_query, rgx_primary_key, "");
	if (!primary_key_string.empty()) {
		// remove the last comma before the last parenthesis from delta_query
		delta_query = std::regex_replace(delta_query, std::regex(R"(,\s*\))"), ")");
	}
	// replace the table name with "delta_table_name"
	delta_query = std::regex_replace(delta_query, std::regex(R"(\b([a-zA-Z0-9_]+)\s*\()"), "delta_$1(");
	// add a new column "multiplicity"
	delta_query = std::regex_replace(delta_query, std::regex(R"(\b\))"), ", _duckdb_ivm_multiplicity boolean)");
	// add a new column "timestamp" with default now()
	delta_query = std::regex_replace(delta_query, std::regex(R"(\b\))"), ", timestamp timestamp default now())");
	// add "if not exists"
	delta_query = std::regex_replace(delta_query, std::regex(R"(\bcreate\s+table\s+)"), "create table if not exists ");
	// todo - bug: this does not work if the primary key is on a single column
	//delta_query = std::regex_replace(delta_query, std::regex(R"(primary\s+key\s*\()"), "primary key(_duckdb_ivm_multiplicity, ");
	delta_query += "\n";
	return delta_query;
}

void CompilerExtension::ReplaceCount(string &query) {
	std::regex pattern("(count\\((\\*|\\w+)\\))(?![^()]*\\bas\\b)", std::regex_constants::icase);
	query = std::regex_replace(query, pattern, "count($2) as count_$2");

	// if count(*) is replaced, change it to count_star
	query = std::regex_replace(query, std::regex("count_\\*"), "count_star");
}

void CompilerExtension::ReplaceSum(string &query) {
	std::regex pattern("(sum\\((\\w+)\\))(?![^()]*\\bas\\b)", std::regex_constants::icase);
	query = std::regex_replace(query, pattern, "sum($2) as sum_$2");
}

} // namespace duckdb
