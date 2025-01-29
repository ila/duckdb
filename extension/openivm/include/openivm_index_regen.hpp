#ifndef DUCKDB_IVM_INDEX_REGEN_HPP
#define DUCKDB_IVM_INDEX_REGEN_HPP

#include "duckdb.hpp"

namespace duckdb {

using old_idx = idx_t;
using new_idx = idx_t;

/// Renumber all table indices that appear in a subtree recursively.
/// A new table index is generated for each operator that usually generates one (GET, AGG, PROJECT, SET operations).
/// Note that this function does not modify the tree.
/// However, it does modify the binder, since the binder is responsible for generating table indices.
/// Returns an unordered map with old_idx -> new_idx for each pair of modified table indices.
std::unordered_map<old_idx, new_idx> RenumberTableIndices(unique_ptr<LogicalOperator>& plan, Binder& binder);

} // namespace duckdb

#endif // DUCKDB_IVM_INDEX_REGEN_HPP