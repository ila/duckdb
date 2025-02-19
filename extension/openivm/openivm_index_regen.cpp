
#include "include/openivm_index_regen.hpp"

#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>

namespace duckdb {

RenumberWrapper renumber_table_indices(unique_ptr<LogicalOperator> plan, Binder &binder) {

	// First, traverse the children, and collect their maps into a singular map.
	std::unordered_map<old_idx, new_idx> table_reassign;
	// Initialise the bindings with the ColumnBindings of the current operator.
	std::vector<ColumnBinding> current_bindings = plan->GetColumnBindings();
	std::vector<unique_ptr<LogicalOperator>> rec_children;
	for (auto& child: plan->children) {
		RenumberWrapper child_wrap = renumber_table_indices(std::move(child), binder);
		table_reassign.insert(child_wrap.idx_map.cbegin(), child_wrap.idx_map.cend());
		// Add the bindings of the children as well.
		current_bindings.insert(
			current_bindings.end(), child_wrap.column_bindings.cbegin(), child_wrap.column_bindings.cend()
        );
		rec_children.emplace_back(std::move(child_wrap.op));
	}

	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// Breaking operator and in general a weird case; only operator with 2 or 3 indices in GetTableIndex.
		unique_ptr<LogicalAggregate> agg_ptr = unique_ptr_cast<LogicalOperator, LogicalAggregate>(std::move(plan));
        // Assuming that assignments are currently valid, reassign the values.
		// Cannot be done in a loop without using pointers, which may be a bit too messy just for this.
		// Group index.
		{
			const idx_t old_gr_idx = agg_ptr->group_index;
			const idx_t new_gr_idx = binder.GenerateTableIndex();
			agg_ptr->group_index = new_gr_idx;
			table_reassign[old_gr_idx] = new_gr_idx;
		}
		// Aggregate index.
		{
			const idx_t old_ag_idx = agg_ptr->group_index;
			const idx_t new_ag_idx = binder.GenerateTableIndex();
			agg_ptr->group_index = new_ag_idx;
			table_reassign[old_ag_idx] = new_ag_idx;
		}
		// Groupings index (if defined).
		{
			const idx_t old_gs_idx = agg_ptr->groupings_index;
			if (old_gs_idx != DConstants::INVALID_INDEX) {
				const idx_t new_gs_idx = binder.GenerateTableIndex();
				agg_ptr->group_index = new_gs_idx;
				table_reassign[old_gs_idx] = new_gs_idx;
			}
		}
		// Move back new children.
		agg_ptr->children = std::move(rec_children);
		return {std::move(agg_ptr), table_reassign, current_bindings};
	}
	case LogicalOperatorType::LOGICAL_GET: {
		// Breaking operator; return only new table index.
		unique_ptr<LogicalGet> get_ptr = unique_ptr_cast<LogicalOperator, LogicalGet>(std::move(plan));
		const idx_t current_idx = get_ptr->table_index;
		const idx_t new_idx = binder.GenerateTableIndex();
		get_ptr->table_index = new_idx;
		table_reassign[current_idx] = new_idx;  // Current map is probably empty at this stage.
		// Logical GET should not have children, so nothing to move.
		return {std::move(get_ptr), table_reassign, current_bindings};
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// Breaking operator; return only new table index.
		unique_ptr<LogicalProjection> proj_ptr = unique_ptr_cast<LogicalOperator, LogicalProjection>(std::move(plan));
		const idx_t current_idx = proj_ptr->table_index;
		const idx_t new_idx = binder.GenerateTableIndex();
		proj_ptr->table_index = new_idx;
		table_reassign[current_idx] = new_idx;
		// Return projection, but add children first.
		proj_ptr->children = std::move(rec_children);
		return {std::move(proj_ptr), table_reassign, current_bindings};
	}
	/*
	// Logical Filter has no GetTableIndex.
	case LogicalOperatorType::LOGICAL_FILTER: {
		// Change the table index based on the mapping.
		unique_ptr<LogicalFilter> filter_ptr = unique_ptr_cast<LogicalOperator, LogicalFilter>(std::move(plan));
		idx_t current_idx = filter_ptr->GetTableIndex();
	}*/
	default: {
#ifdef DEBUG
		printf("table indices of type %s ignored.\n", LogicalOperatorToString(plan->type).c_str());
#endif
		break;
	}
	}
	// Default return value (when switch doesn't change anything) is current_map.
	plan->children = std::move(rec_children);
	return {std::move(plan), table_reassign, current_bindings};
}

ColumnBindingReplacer vec_to_replacer(
	const std::vector<ColumnBinding>& bindings, const std::unordered_map<old_idx, new_idx>& table_mapping
) {
	std::unordered_map<old_idx, std::unordered_set<col_idx>> to_replace;
	// We only need to include those bindings whose tables are in the table mapping.
	for (const ColumnBinding col_binding : bindings) {
		idx_t table_index = col_binding.table_index;
		if (table_mapping.find(table_index) != table_mapping.end()) {
			// Binding's table index will be replaced!
			to_replace[table_index].insert(col_binding.column_index);
		}
	}
	// Now that all bindings are checked, let's create a ColumnBindingReplacer!
	ColumnBindingReplacer replacer;
	for (const auto& pair : to_replace) {
		const old_idx old_t = pair.first;
		const new_idx new_t = table_mapping.at(old_t);
		for (const col_idx col : pair.second) {
			const auto old_binding = ColumnBinding(old_t, col);
			const auto new_binding = ColumnBinding(new_t, col);
			replacer.replacement_bindings.emplace_back(old_binding, new_binding);
		}
	}
	return replacer;
}

} // namespace duckdb
