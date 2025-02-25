#pragma once

#include "duckdb.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "flush_function.hpp"

namespace duckdb {

class ServerExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	string Name() override;
};

} // namespace duckdb
