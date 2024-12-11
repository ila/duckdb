#ifndef DUCKDB_OPENIVM_UPSERT_HPP
#define DUCKDB_OPENIVM_UPSERT_HPP

#pragma once

#include "duckdb.hpp"

namespace duckdb {

enum class IVMType : uint8_t { AGGREGATE_GROUP, SIMPLE_AGGREGATE, SIMPLE_PROJECTION };

string UpsertDeltaQueries(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb



#endif // DUCKDB_OPENIVM_UPSERT_HPP
