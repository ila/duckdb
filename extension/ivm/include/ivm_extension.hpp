#ifndef IVM_EXTENSION_HPP
#define IVM_EXTENSION_HPP

#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "ivm_parser.hpp"
#include "ivm_insert_rule.hpp"
#include "ivm_rewrite_rule.hpp"

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

// config.operator_extensions.push_back(duckdb::make_uniq<RemoteLocalOperatorExtension>());

namespace duckdb {

class IvmExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb

#endif
