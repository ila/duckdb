//
// Created by ila on 4/2/25.
//

#ifndef GENERATE_PYTHON_SCRIPT_HPP
#define GENERATE_PYTHON_SCRIPT_HPP

#include "duckdb.hpp"

namespace duckdb {

void GenerateServerRefreshScript(ClientContext &context, const FunctionParameters &parameters);
void GenerateClientRefreshScript(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb

#endif // GENERATE_PYTHON_SCRIPT_HPP
