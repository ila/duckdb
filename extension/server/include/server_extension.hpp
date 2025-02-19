#pragma once

#include "duckdb.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "centralized_view_optimizer_rule.hpp"

namespace duckdb {

struct FlushFunctionData : TableFunctionData {
	FlushFunctionData() {
	}
};

struct FlushData : GlobalTableFunctionState {
	FlushData() : offset(0) {
	}
	idx_t offset;
	string view_name;
	string min_agg_col_name;
	int min_agg_value = 0;
	LogicalType min_agg_type;
};

class ServerExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	string Name() override;
};

} // namespace duckdb
