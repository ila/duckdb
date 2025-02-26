#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ClientExtension : public Extension {
public:
	void CloseConnection(int32_t sock);
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
