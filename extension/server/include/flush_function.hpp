//
// Created by ila on 2/7/25.
//

#ifndef FLUSH_FUNCTION_HPP
#define FLUSH_FUNCTION_HPP
#include <duckdb/function/function.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <rdda_parser.hpp>

namespace duckdb {
    void FlushFunction(ClientContext &context, const FunctionParameters &parameters);
};


#endif //FLUSH_FUNCTION_HPP
