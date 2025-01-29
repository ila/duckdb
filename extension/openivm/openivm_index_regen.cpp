
#include "include/openivm_index_regen.hpp"

#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_get.hpp>

namespace duckdb {

std::unordered_map<old_idx, new_idx> RenumberTableIndices(unique_ptr<LogicalOperator> &plan, Binder &binder) {

	// First, traverse the children, and collect their maps into a singular map.
	std::unordered_map<old_idx, new_idx> current_map;
	for (auto child: plan->children) {
		auto child_map = RenumberTableIndices(child, binder);
		current_map.insert(child_map.begin(), child_map.end());
	}

	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// Breaking operator and in general a weird case; only operator with 2 or 3 indices in GetTableIndex.
		unique_ptr<LogicalAggregate> agg_ptr = unique_ptr_cast<LogicalOperator, LogicalAggregate>(std::move(plan));
		auto agg_reassign_map = std::unordered_map<idx_t, idx_t>();
        // Assuming that assignments are currently valid, reassign the values.
		// Cannot be done in a loop without using pointers, which may be a bit too messy just for this.
		// Group index.
		{
			const idx_t old_gr_idx = agg_ptr->group_index;
			const idx_t new_gr_idx = binder.GenerateTableIndex();
			agg_ptr->group_index = new_gr_idx;
			agg_reassign_map[old_gr_idx] = new_gr_idx;
		}
		// Aggregate index.
		{
			const idx_t old_ag_idx = agg_ptr->group_index;
			const idx_t new_ag_idx = binder.GenerateTableIndex();
			agg_ptr->group_index = new_ag_idx;
			agg_reassign_map[old_ag_idx] = new_ag_idx;
		}
		// Groupings index (if defined).
		{
			const idx_t old_gs_idx = agg_ptr->groupings_index;
			if (old_gs_idx != DConstants::INVALID_INDEX) {
				const idx_t new_gs_idx = binder.GenerateTableIndex();
				agg_ptr->group_index = new_gs_idx;
				agg_reassign_map[old_gs_idx] = new_gs_idx;
			}
		}
		return agg_reassign_map;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		// Breaking operator; return only new table index.
		unique_ptr<LogicalGet> get_ptr = unique_ptr_cast<LogicalOperator, LogicalGet>(std::move(plan));
		const idx_t current_idx = get_ptr->table_index;
		const idx_t new_idx = binder.GenerateTableIndex();
		auto ret_map = std::unordered_map<idx_t, idx_t>();
		ret_map[current_idx] = new_idx;
		return ret_map;
	}
	default:
		throw NotImplementedException("Operator type %s not supported", LogicalOperatorToString(plan->type));
	}

	// TODO: replace with proper return value.
	return std::unordered_map<idx_t, idx_t>();
}
} // namespace duckdb
