#ifndef DUCKDB_OPENIVM_UPSERT_HPP
#define DUCKDB_OPENIVM_UPSERT_HPP

#pragma once

#include "duckdb.hpp"

namespace duckdb {

enum class IVMType : uint8_t {
	AGGREGATE_GROUP,
	SIMPLE_FILTER,
	SIMPLE_AGGREGATE,
	SIMPLE_PROJECTION,
	SIMPLE_JOIN,
	AGGREGATE_JOIN,
};

string UpsertDeltaQueries(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb



#endif // DUCKDB_OPENIVM_UPSERT_HPP
