#define DUCKDB_EXTENSION_MAIN

#include "differential_privacy_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "dp_parser.hpp"

namespace duckdb {
// TODO remove (kept for some debugging)
inline void DifferentialPrivacyScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Quack " + name.GetString());
		;
	});
}

static void LoadInternal(DatabaseInstance &instance) {

	// TODO remove (kept for some debugging)
	// Register a scalar function
	auto differential_privacy_scalar_function =
	    ScalarFunction("quack", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DifferentialPrivacyScalarFun);
	ExtensionUtil::RegisterFunction(instance, differential_privacy_scalar_function);

	// add a parser extension
	auto &db_config = duckdb::DBConfig::GetConfig(instance);
	auto DP_parser = duckdb::DPParserExtension();
	db_config.parser_extensions.push_back(DP_parser);
}

void DifferentialPrivacyExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string DifferentialPrivacyExtension::Name() {
	return "differential_privacy";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void differential_privacy_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::DifferentialPrivacyExtension>();
}

DUCKDB_EXTENSION_API const char *differential_privacy_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
