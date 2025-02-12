#ifndef DUCKDB_IVM_INDEX_REGEN_HPP
#define DUCKDB_IVM_INDEX_REGEN_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

using old_idx = idx_t;
using new_idx = idx_t;
using col_idx = idx_t;

struct RenumberWrapper {
	/// The operator (which should be returned as a unique pointer).
	unique_ptr<LogicalOperator> op;
	/// The table index mapping (from old to new).
	std::unordered_map<old_idx, new_idx> idx_map;
	/// Vector of ColumnBindings. Used to eventually create a ColumnBindingReplacer.
	/// Unsorted and may contain duplicates, so take care of that when creating the replacer.
	// This can be done more efficiently with maps and sets,
	// but since a ColumnBinding cannot be hashed this is a bit messier to implement.
	std::vector<ColumnBinding> column_bindings;
};

/// Renumber all table indices that appear in a subtree recursively.
/// A new table index is generated for each operator that usually generates one (GET, AGG, PROJECT, SET operations).
/// Note that this function does not modify the tree.
/// However, it does modify the binder, since the binder is responsible for generating table indices.
/// Returns an unordered map with old_idx -> new_idx for each pair of modified table indices.
RenumberWrapper renumber_table_indices(unique_ptr<LogicalOperator> plan, Binder& binder);

/// Create a ColumnBindingReplacer using a vector of ColumnBindings, and a mapping of old->new table indices.
/// The bindings in the vector do not need to be unique or sorted.
/// This function will take care of only using distinct bindings.
ColumnBindingReplacer vec_to_replacer(const std::vector<ColumnBinding>& bindings, const std::unordered_map<old_idx, new_idx>& table_mapping);


} // namespace duckdb

#endif // DUCKDB_IVM_INDEX_REGEN_HPP