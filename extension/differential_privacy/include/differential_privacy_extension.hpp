#pragma once

#include "duckdb.hpp"
#include "dp_parser.hpp"

namespace duckdb {

class DifferentialPrivacyExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
