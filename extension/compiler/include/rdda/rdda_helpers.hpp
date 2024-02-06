//
// Created by ila on 24-10-23.
//

#ifndef DUCKDB_RDDA_HELPERS_HPP
#define DUCKDB_RDDA_HELPERS_HPP

#include "duckdb.hpp"

namespace duckdb {

// helpers containing the newly introduced keywords
enum class TableScope { null = 0, centralized = 1, decentralized = 2, replicated = 3 };

struct RDDAConstraint {
	string column_name;
	bool randomized = false;
	int minimum_aggregation = 0;
	bool sensitive = false;
};

struct RDDAViewConstraint {
	uint8_t window = 0;
	uint8_t ttl = 0;
	uint8_t refresh = 0;
};

struct RDDASelectOption {
	float response_ratio = -1;
	float minimum_response = -1;
};
} // namespace duckdb

#endif // DUCKDB_RDDA_HELPERS_HPP
