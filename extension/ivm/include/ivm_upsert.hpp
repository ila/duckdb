#ifndef DUCKDB_IVM_UPSERT_HPP
#define DUCKDB_IVM_UPSERT_HPP

#pragma once

#include "duckdb.hpp"

namespace duckdb {

enum class IVMType : uint8_t { AGGREGATE_GROUP, SIMPLE_FILTER, SIMPLE_AGGREGATE, SIMPLE_PROJECTION };

string UpsertDeltaQueries(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb



#endif // DUCKDB_IVM_UPSERT_HPP
