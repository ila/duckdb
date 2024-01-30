#ifndef IVM_EXTENSION_HPP
#define IVM_EXTENSION_HPP

#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "openivm_insert_rule.hpp"
#include "openivm_parser.hpp"
#include "openivm_rewrite_rule.hpp"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

// config.operator_extensions.push_back(duckdb::make_uniq<RemoteLocalOperatorExtension>());

namespace duckdb {

class OpenivmExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb

#endif
