#include "openivm_rewrite_rule.hpp"

// From DuckDB.
#include "../../compiler/include/logical_plan_to_string.hpp"
#include "../../postgres_scanner/include/postgres_scanner.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb.hpp"
#include <duckdb/optimizer/column_binding_replacer.hpp>
#include <duckdb/planner/operator/logical_comparison_join.hpp>
#include "openivm_index_regen.hpp"

// Std.
#include "../../third_party/zstd/include/zstd/common/debug.h"
#include <iostream>

namespace {
using duckdb::vector;
using duckdb::ColumnBinding;
using duckdb::Expression;
using duckdb::BoundColumnRefExpression;
using duckdb::LogicalType;
using duckdb::unique_ptr;
using duckdb::make_uniq;
using duckdb::JoinCondition;

/// Adjust the column order for a join between Delta-L and R.
/// This vector of Expressions is meant to be used in conjunction with a LogicalProjection.
vector<unique_ptr<Expression>> project_dl_r_join(
    const vector<ColumnBinding>& dl_bindings, const vector<LogicalType>& dl_types,
    const vector<ColumnBinding>& r_bindings, const vector<LogicalType>& r_types
) {
	// Contains the column IDs of within the LEFT side of the projection.
	// From there, we want everything but the last element to stay in order.
	const size_t dl_col_count = dl_bindings.size();
	const size_t r_col_count = r_bindings.size();
	assert(dl_col_count == dl_types.size());
	assert(r_col_count == r_types.size());

	const size_t projection_col_count = dl_col_count + r_col_count;
	// Note: slots are already made here, only need to be "populated".
	auto projection_col_refs = vector<unique_ptr<Expression>>(projection_col_count);
	// TODO: Do we need to do anything with the `left/right_projection_map`?
	/*
		 * What is done here:
		 * For all except of the last element of L's column bindings,
		 *  the respective ColumnBinding is added to the projection.
		 * The last element of L's bindings (the multiplicity column) is specially kept to insert last.
	 */
	// Mind the `-1`: the last element does not get added yet but only at the very end.
	const idx_t last_dl_col_idx = dl_col_count - 1;
	for (idx_t i = 0; i < last_dl_col_idx; ++i ) {
		projection_col_refs[i] = make_uniq<BoundColumnRefExpression>(dl_types[i], dl_bindings[i]);
	}
	// Insert the multiplicity column at the end.
	projection_col_refs[projection_col_count - 1] = make_uniq<BoundColumnRefExpression>(
	    dl_types[last_dl_col_idx], dl_bindings[last_dl_col_idx]
	);
	// Now insert everything of R. Here, everything is inserted, so no special increment cutoff.
	for (idx_t i = 0; i < r_col_count; ++i) {
		// First index here should be where dL left off.
		// Omitting the mul column, that is therefore `last_dl_col_idx` (which is unoccupied).
		// Note that `i = 0`, and thus `last_dl_col_idx + i` = `last_dl_col_idx` (which is intended).
		projection_col_refs[last_dl_col_idx + i] = make_uniq<BoundColumnRefExpression>(r_types[i], r_bindings[i]);
	}
	return projection_col_refs;
}

/// Project out the multiplicity column of the `dl` table.
vector<unique_ptr<Expression>> project_dl_dr_join(
    const vector<ColumnBinding>& dl_bindings, const vector<LogicalType>& dl_types,
    const vector<ColumnBinding>& dr_bindings, const vector<LogicalType>& dr_types
) {
	const size_t dl_col_count = dl_bindings.size();
	const size_t dr_col_count = dr_bindings.size();
	assert(dl_col_count == dl_types.size());
	assert(dr_col_count == dr_types.size());

	// Create the vec and reserve the amount of elements (but don't create them yet).
	auto projection_col_refs = vector<unique_ptr<Expression>>();
	projection_col_refs.reserve(dl_col_count + dr_col_count - 1); // -1, since left mul removed.
	// Insert the columns of dL. Mind the `-1`: the last element is omitted.
	for (idx_t i = 0; i < dl_col_count - 1; ++i ) {
		projection_col_refs.emplace_back(make_uniq<BoundColumnRefExpression>(dl_types[i], dl_bindings[i]));
	}
	// Insert all columns of dR.
	for (idx_t i = 0; i < dr_col_count; ++i) {
		projection_col_refs.emplace_back(make_uniq<BoundColumnRefExpression>(dr_types[i], dr_bindings[i]));
	}
	return projection_col_refs;
}

/// Create an extra join condition for dL JOIN dR on the multiplicity column.
JoinCondition create_mul_join_condition(
    const vector<ColumnBinding>& dl_bindings, const vector<LogicalType>& dl_types,
    const vector<ColumnBinding>& dr_bindings, const vector<LogicalType>& dr_types
) {
	auto dl_tail = dl_bindings.size() - 1;
	auto dr_tail = dr_bindings.size() - 1;
	// Create the join condition.
	JoinCondition eq_condition;
	eq_condition.left = make_uniq<BoundColumnRefExpression>("left_mul", dl_types[dl_tail], dl_bindings[dl_tail], 0);
	eq_condition.right = make_uniq<BoundColumnRefExpression>("right_mul", dr_types[dr_tail], dr_bindings[dr_tail], 0);
	eq_condition.comparison = duckdb::ExpressionType::COMPARE_EQUAL;
	return eq_condition;
}
} // namespace


namespace duckdb {

void IVMRewriteRule::AddInsertNode(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                   string &view_name, string &view_catalog_name, string &view_schema_name) {
#ifdef DEBUG
	printf("\nAdd the insert node to the plan...\n");
	printf("Plan:\n%s\nParameters:", plan->ToString().c_str());
	// Get whatever ParameterToString yields.
	for (const auto& i_param : plan->ParamsToString()) {
		printf("%s", i_param.second.c_str());
	}
	printf("\n---end of insert node output---\n");
#endif

	auto delta_table_catalog_entry =
	    Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, view_catalog_name, view_schema_name,
	                      "delta_" + view_name, OnEntryNotFound::RETURN_NULL, QueryErrorContext());
	optional_ptr<TableCatalogEntry> table = dynamic_cast<TableCatalogEntry *>(delta_table_catalog_entry.get());
	// create insert node. It is new node, hence it gets a new table_idx
	// putting an arbitrary index here
	// todo -- do we even need this insert node on top?
	auto insert_node = make_uniq<LogicalInsert>(*table, 999);

	// generate bindings for the insert node using the top node of the plan
	Value value;
	unique_ptr<BoundConstantExpression> exp;
	for (size_t i = 0; i < plan->expressions.size(); i++) {
		insert_node->expected_types.emplace_back(plan->expressions[i]->return_type);
		value = Value(plan->expressions[i]->return_type);
		exp = make_uniq<BoundConstantExpression>(std::move(value));
		insert_node->bound_defaults.emplace_back(std::move(exp));
	}

	// insert the insert node at the top of the plan
	insert_node->children.emplace_back(std::move(plan));
	plan = std::move(insert_node);
}

ModifiedPlan IVMRewriteRule::ModifyPlan(PlanWrapper pw) {
	ClientContext &context = pw.input.context;
	/*
	 * For join support, create a copy of both children for use later.
	 * The reason is that both the delta "state" and the original "state" of each child are needed.
	 * Without a copy, the original (non-delta) state of the children would be lost during recursion.
	 */
	unique_ptr<LogicalOperator> left_child, right_child;
	if (pw.plan.get()->type == LogicalOperatorType::LOGICAL_JOIN ||
	    pw.plan.get()->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		left_child = pw.plan->children[0]->Copy(context);
		right_child = pw.plan->children[1]->Copy(context);
		// TODO: are those needed here? Check!
		left_child->ResolveOperatorTypes();
		right_child->ResolveOperatorTypes();
	}
	std::vector<ColumnBinding> child_mul_bindings;
	// Call each child of `plan` recursively (depth-first).
	for (auto &&child : pw.plan->children) {
		auto rec_pw = PlanWrapper(pw.input, child, pw.view, pw.root);
		ModifiedPlan child_plan = ModifyPlan(rec_pw);
		// No need to do anything with child_plan.op, as this should already be modified in pw.plan->children).
		// FIXME: Verify comment above.
		child_mul_bindings.emplace_back(child_plan.mul_binding);
	}
	QueryErrorContext error_context = QueryErrorContext();

	// Rewrite operators depending on their type.
	switch (pw.plan->type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	{
		// Store the table indices of the left and right child for usage in column binding replacer much later.
		// Needed here, because the left and right child will eventually be `std::move`d.
		vector<ColumnBinding> lc_binds = left_child->GetColumnBindings();
		vector<ColumnBinding> rc_binds = right_child->GetColumnBindings();
		/* Ensure that the resulting types of each join is consistent.
		 * To help with that, create a copy of the `types` of pw.plan (which is a vec of logicaltype).
		 * This should be equivalent to the types of `L.*, R.*`
		 * The union, however, assumes the columns to be equivalent to `L.*, R.*, mul`.
		 * This means that mul must be added at some point before the union takes place.
		 */
		auto types = pw.plan->types;
		types.emplace_back(pw.mul_type); // Add bool type for multiplicity.
		// Cast plan into a LogicalComparisonJoin representing dL JOIN dR.
		unique_ptr<LogicalComparisonJoin> join_dl_dr = unique_ptr_cast<LogicalOperator, LogicalComparisonJoin>(std::move(pw.plan));
		printf("Modified plan (join, start, post-cast):\n%s\nParameters:", join_dl_dr->ToString().c_str());
		for (const auto& i_param : join_dl_dr->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
#ifdef DEBUG
		printf("join detected. join child count: %zu\n", join_dl_dr->children.size());
		printf("plan left_child child count: %zu\n", left_child->children.size());
		printf("plan right_child child count: %zu\n", right_child->children.size());
#endif
		if (join_dl_dr->join_type != JoinType::INNER) {
			throw Exception(ExceptionType::OPTIMIZER, JoinTypeToString(join_dl_dr->join_type) + " type not yet supported in OpenIVM");
		}
		auto og_join_bindings = join_dl_dr->GetColumnBindings();
		/* Suppose the query tree (below this join) as an L-side (left child) and an R side (right child).
		 * Before any modification, the query simply joins "current" L with "current" R.
		 * However, now, this query should act as an *update* to the original view.
		 * To this end, a comparison between `current L` and `current R` is not needed.
		 * Rather, this query should yield the union of the following result sets:
		 * 1. `delta L` joined with `current R` -> `join_dl_r`
		 * 2. `current L` joined with `delta R` -> `join_l_dr`
		 * 3. `delta L` joined with `delta R`, iff the multiplicity matches -> `join`
		 *
		 * The code below adapts the tree such that `current L` JOIN `current R` is modified to the 3 joins of interest.
		 */
		// Make a copy of a join between deltas. This copy will eventually become `delta L` with `current R`.
		unique_ptr<LogicalComparisonJoin> join_dl_r = unique_ptr_cast<LogicalOperator, LogicalComparisonJoin>(join_dl_dr->Copy(context));
		// Remove the deltas: join_dl_r temporarily has no children.
		auto left_delta = std::move(join_dl_r->children[0]);
		auto right_delta = std::move(join_dl_r->children[1]);
		// Copy the (now child-less) join, to form the basis for `join_l_dr`.
		unique_ptr<LogicalComparisonJoin> join_l_dr = unique_ptr_cast<LogicalOperator, LogicalComparisonJoin>(join_dl_r->Copy(context));

		// Give these two joins their appropriate children.
		join_dl_r->children[0] = std::move(left_delta);
		join_dl_r->children[1] = std::move(right_child);
		join_dl_r->types = types;
		join_l_dr->children[0] = std::move(left_child);
		join_l_dr->children[1] = std::move(right_delta);
		join_l_dr->types = types;
#ifdef DEBUG
		printf("`delta L` JOIN `R` child count: %zu\n", join_dl_r->children.size());
		printf("`L` JOIN `delta R` child count: %zu\n", join_l_dr->children.size());
		printf("`delta L` JOIN `delta R` child count: %zu\n", join_dl_dr->children.size());
#endif
		// As we have a join, there should be EXACTLY two children of the plan, and thus two multiplicity columns.
		const ColumnBinding og_dl_mul = child_mul_bindings[0];
		const ColumnBinding og_dr_mul = child_mul_bindings[1];

		// Renumber the table indices of each DELTA left/right child, and make projections where necessary.
		// (1) dLdR: renumber both, and ensure only dR's multiplicity column is in the projection.
		unique_ptr<LogicalProjection> projection_dl_dr;
		{
			auto res_l = renumber_table_indices(std::move(join_dl_dr->children[0]), pw.input.optimizer.binder);
			auto res_r = renumber_table_indices(std::move(join_dl_dr->children[1]), pw.input.optimizer.binder);
			join_dl_dr->children[0] = std::move(res_l.op);
			join_dl_dr->children[1] = std::move(res_r.op);
			// Run two ColumnBindingReplacers (one for each child) on the level of the join.
			// Caveat of doing it this way: both times you traverse one child for nothing.
			// But hey, if it yields the correct result it yields the correct result.
			ColumnBindingReplacer dl_replacer = vec_to_replacer(res_l.column_bindings, res_l.idx_map);
			ColumnBindingReplacer dr_replacer = vec_to_replacer(res_r.column_bindings, res_r.idx_map);
			dl_replacer.VisitOperator(*join_dl_dr);
			dr_replacer.VisitOperator(*join_dl_dr);

			// Project out dL-mul.
			// First, add an additional join condition (dL.mul = dR.mul).
			{
				ColumnBinding dl_mul = ColumnBinding(res_l.idx_map[og_dl_mul.table_index], og_dl_mul.column_index);
				ColumnBinding dr_mul = ColumnBinding(res_r.idx_map[og_dr_mul.table_index], og_dr_mul.column_index);

				join_dl_dr->ResolveOperatorTypes();
				auto join_bindings = join_dl_dr->GetColumnBindings();
				auto join_types = join_dl_dr->types;
				// Note: in its own block, to avoid potential conflicts with join_dl_r ones.
				// Get the vectors and their length.
				vector<ColumnBinding> dl_bindings = join_dl_dr->children[0]->GetColumnBindings();
				vector<LogicalType> dl_types = join_dl_dr->children[0]->types;
				vector<ColumnBinding> dr_bindings = join_dl_dr->children[1]->GetColumnBindings();
				vector<LogicalType> dr_types = join_dl_dr->children[1]->types;
				// Create the join condition.
				join_dl_dr->conditions.emplace_back(
				    create_mul_join_condition(dl_bindings, dl_types, dr_bindings, dr_types));
				// Get rid of dL's column using a projection.
				vector<unique_ptr<Expression>> dl_dr_projection_bindings =
				    project_dl_dr_join(dl_bindings, dl_types, dr_bindings, dr_types);
				projection_dl_dr = make_uniq<LogicalProjection>(
				    pw.input.optimizer.binder.GenerateTableIndex(),
				    std::move(dl_dr_projection_bindings));
			}
		}
		// (2) dLR: Renumber dL, and project dL's multiplicity column to the end.
		unique_ptr<LogicalProjection> projection_dl_r;
		{
			auto res = renumber_table_indices(std::move(join_dl_r->children[0]), pw.input.optimizer.binder);
			join_dl_r->children[0] = std::move(res.op);
			// Run a ColumnBindingReplacer after moving the operator, such that the join itself also gets replacements.
			ColumnBindingReplacer replacer = vec_to_replacer(res.column_bindings, res.idx_map);
			replacer.VisitOperator(*join_dl_r);
			{
				// Resolve the operator types, so that it clear what types the columns in the join have.
				join_dl_r->ResolveOperatorTypes();
				// FIXME: Remove this (it's for debugging purposes).
				auto join_bindings = join_dl_r->GetColumnBindings();
				/*
				 * We now have dL R, and we need to move the multiplicity column to the end of the column bindings.
				 * We know the multiplicity binding of dL, since it is the first column in child_mul_bindings.
				 * All that we need to do is to "capture" that binding and hold it for the end.
				 */
				vector<ColumnBinding> dl_bindings = join_dl_r->children[0]->GetColumnBindings();
				vector<LogicalType> dl_types = join_dl_r->children[0]->types;
				vector<ColumnBinding> r_bindings = join_dl_r->children[1]->GetColumnBindings();
				vector<LogicalType> r_types = join_dl_r->children[1]->types;
				vector<unique_ptr<Expression>> dl_r_projection_bindings =
				    project_dl_r_join(dl_bindings, dl_types, r_bindings, r_types);
				// Now, the vector with bindings should be complete. Let's put it in a Projection node!
				projection_dl_r = make_uniq<LogicalProjection>(
				    pw.input.optimizer.binder.GenerateTableIndex(),
				    std::move(dl_r_projection_bindings)
				);
			}
		}
		// (3) LdR: renumber dR, but don't do anything with projections.
		{
			auto res = renumber_table_indices(std::move(join_l_dr->children[1]), pw.input.optimizer.binder);
			join_l_dr->children[1] = std::move(res.op);
			// Run a ColumnBindingReplacer after moving the operator, such that the join itself also gets replacements.
			ColumnBindingReplacer replacer = vec_to_replacer(res.column_bindings, res.idx_map);
			replacer.VisitOperator(*join_l_dr);

			// LdR projection -> keep as-is.
			/* Don't do anything here */
		}
		projection_dl_r->children.emplace_back(std::move(join_dl_r));
		projection_dl_dr->children.emplace_back(std::move(join_dl_dr));

		// Now that all joins have the same columns, create a Union!
		auto copy_union = make_uniq<LogicalSetOperation>(
		    pw.input.optimizer.binder.GenerateTableIndex(),
		    types.size(),
		    std::move(projection_dl_r),
		    std::move(join_l_dr),  // No projection needed, multiplicity column on the right place.
		    LogicalOperatorType::LOGICAL_UNION,
		    true
		);
		copy_union->types = types;
		auto upper_u_table_index = pw.input.optimizer.binder.GenerateTableIndex();
		pw.plan = make_uniq<LogicalSetOperation>(
		    upper_u_table_index,
		    types.size(),
		    std::move(copy_union),
		    std::move(projection_dl_dr),
		    LogicalOperatorType::LOGICAL_UNION,
		    true
		);
		pw.plan->types = types;
		printf("Modified plan (join, end):\n%s\nParameters:", pw.plan->ToString().c_str());
		for (const auto& i_param : pw.plan->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		// Rebind everything, because new joins have been implemented.
		ColumnBinding new_mul_binding;
		{
			ColumnBindingReplacer replacer;
			vector<ReplacementBinding>& replacement_bindings = replacer.replacement_bindings;
			const auto bindings = pw.plan->GetColumnBindings();
			// Old bindings get rebound to the union's columns, where everything goes from left to right.
			// To form the old bindings, use the original left child and right child of the original join.
			vector<ColumnBinding> old_bindings = lc_binds;
			old_bindings.insert(old_bindings.end(), rc_binds.begin(), rc_binds.end());
			// `-1`, because multiplicity column is not part of the ColumnBindingReplacer (but handled right after).
			idx_t mul_col_idx = bindings.size() - 1;
			for (idx_t col_idx = 0; col_idx < mul_col_idx ; col_idx++) {
				// Old binding should be 0.0 or 1.0 something.
				const auto &old_binding = old_bindings[col_idx];
				const auto &new_binding = ColumnBinding(upper_u_table_index, col_idx);
				replacement_bindings.emplace_back(old_binding, new_binding);
			}
#ifdef DEBUG
			// Print the replacement bindings.
			printf("\n--- Running a ColumnBindingReplacer after the Union ---\n");
			for (const auto& i_binding : replacement_bindings) {
				// Split up in two because of encoding issues.
				printf("old binding %s -> ", (i_binding.old_binding.ToString().c_str()));
				printf("new binding %s\n", (i_binding.new_binding.ToString().c_str()));
			}
#endif
			replacer.stop_operator = pw.plan;
			replacer.VisitOperator(*pw.root);
			/* Finally, change the ColumnBinding of the multiplicity column, and assign it to new_mul_binding.
			 * This will be used by later steps of ModifyPlan (including ModifyTopNode)
			 * to add the multiplicity column to wherever needed (mainly projections).
			 * Once again, it is assumed that the multiplicity column is at the END of the bindings.
			 */
			new_mul_binding = {upper_u_table_index, mul_col_idx};
#ifdef DEBUG
			printf("The new multiplicity binding shall be %s.\n", (new_mul_binding.ToString().c_str()));
			printf("--- End of ColumnBindingReplacer ---\n");
#endif
		}
		return {std::move(pw.plan), new_mul_binding};
	}
	/*
	 * END OF CASE.
	 */
	case LogicalOperatorType::LOGICAL_GET: {
		// we are at the bottom of the tree
		auto old_get = dynamic_cast<LogicalGet *>(pw.plan.get());
		// FIXME: Should the types be set here?

#ifdef DEBUG
		printf("Create replacement get node \n");
#endif
		string delta_table;
		string delta_table_schema;
		string delta_table_catalog;
		// checking if the table to be scanned exists in DuckDB
		if (old_get->GetTable().get() == nullptr) {
			// we are using PostgreSQL (the underlying table does not exist)
			delta_table = "delta_" + dynamic_cast<PostgresBindData *>(old_get->bind_data.get())->table_name;
			delta_table_schema = "public";
			delta_table_catalog = "p"; // todo
		} else {
			// DuckDB (default case)
			delta_table = "delta_" + old_get->GetTable().get()->name;
			delta_table_schema = old_get->GetTable().get()->schema.name;
			delta_table_catalog = old_get->GetTable().get()->catalog.GetName();
		}
		auto table_catalog_entry =
		    Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, delta_table_catalog, delta_table_schema,
		                      delta_table, OnEntryNotFound::RETURN_NULL, error_context);
		if (table_catalog_entry == nullptr) {
			// if delta base table does not exist, return error
			// this also means there are no deltas to compute
			throw Exception(ExceptionType::BINDER, "Table " + delta_table + " does not exist, no deltas to compute!");
		}

		// we are replacing the GET node with a new GET node that reads the delta table
		// this logic is a bit wonky, the plan should not be executed after these changes
		// however, this is fed to LPTS, which is good enough to generate the query string
		// the previous implementation added a projection on top of the new scan, and scanned all the columns
		// however, I find the code a bit complicated and harder to turn into a string
		// so, now we go with this solution and pray it won't break

		auto &table_entry = table_catalog_entry->Cast<TableCatalogEntry>();
		unique_ptr<FunctionData> bind_data;
		auto scan_function = table_entry.GetScanFunction(context, bind_data);
		vector<LogicalType> return_types = {};
		vector<string> return_names = {};
		vector<ColumnIndex> column_ids = {};

		// the delta table has the same columns and column names as the base table, in the same order
		// therefore, we just need to add the columns that we need
		// this is ugly, but needs to stay like this
		// sometimes DuckDB likes to randomly invert columns, so we need to check all of them
		// example: a SELECT * can be translated to 1, 0, 2, 3 rather than 0, 1, 2, 3
		for (auto &id : old_get->GetColumnIds()) {
			column_ids.push_back(id);
			for (auto &col : table_entry.GetColumns().Logical()) {
				if (col.Oid() == id.GetPrimaryIndex()) {
					return_types.push_back(col.Type());
					return_names.push_back(col.Name());
				}
			}
		}

		// we also need to add the multiplicity column
		return_types.push_back(pw.mul_type);
		return_names.push_back("_duckdb_ivm_multiplicity");
		auto idx = ColumnIndex(table_entry.GetColumns().GetColumnTypes().size() - 2);
		column_ids.push_back(idx);

		ColumnBinding new_mul_binding = ColumnBinding(old_get->table_index, column_ids.size() - 1);
		// we also add the timestamp column
		return_types.push_back(LogicalType::TIMESTAMP);
		return_names.push_back("timestamp");
		//column_ids.push_back(table_entry.GetColumns().GetColumnTypes().size() - 1);
		idx = ColumnIndex(table_entry.GetColumns().GetColumnTypes().size() - 1);
		column_ids.push_back(idx);

		// the new get node that reads the delta table gets a new table index
		unique_ptr<LogicalGet> replacement_get_node = make_uniq<LogicalGet>(
		    // NOTE: "New table index" -> but inherits old one? so -> pw.input.optimizer.binder.GenerateTableIndex()
		    old_get->table_index,
		    scan_function,
		    std::move(bind_data),
		    std::move(return_types),
		    std::move(return_names)
		);
		replacement_get_node->SetColumnIds(std::move(column_ids));
		replacement_get_node->table_filters = std::move(old_get->table_filters); // this should be empty

		// FIXME: Why add the filter if there is no filter in the plan???
		// we add the filter for the timestamp if there is no filter in the plan
		Connection con(*context.db);
		con.SetAutoCommit(false);
		// we add a table filter
		auto timestamp_query = "select last_update from _duckdb_ivm_delta_tables where view_name = '" + pw.view + "' and table_name = '" + table_entry.name + "';";
		auto r = con.Query(timestamp_query);
		if (r->HasError()) {
			throw InternalException("Error while querying last_update");
		}
		auto timestamp_column = make_uniq<BoundColumnRefExpression>(
		    "timestamp", LogicalType::TIMESTAMP,
		    ColumnBinding(new_mul_binding.table_index, new_mul_binding.column_index + 1));

		auto table_filter = make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, r->GetValue(0, 0));
		replacement_get_node->table_filters.filters[new_mul_binding.column_index + 1] = std::move(table_filter);

		replacement_get_node->projection_ids = old_get->projection_ids;
		// Add the multiplicity column to the projection IDs.
		// The size is "past the end" of the old get projection IDs, and thus the projection ID of the mul column,
		replacement_get_node->projection_ids.emplace_back(old_get->projection_ids.size());
		replacement_get_node->ResolveOperatorTypes();
#ifdef DEBUG
		auto whatever = replacement_get_node->GetColumnBindings();
#endif
		return {std::move(replacement_get_node), new_mul_binding};
	}
	/*
	 * END OF CASE.
	 */
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {

		auto modified_node_logical_agg = dynamic_cast<LogicalAggregate *>(pw.plan.get());
#ifdef DEBUG
		for (size_t i = 0; i < modified_node_logical_agg->GetColumnBindings().size(); i++) {
			printf("aggregate node CB before %zu %s\n", i,
			       modified_node_logical_agg->GetColumnBindings()[i].ToString().c_str());
		}
		printf("Aggregate index: %lu Group index: %lu\n", modified_node_logical_agg->aggregate_index,
		       modified_node_logical_agg->group_index);
#endif

		ColumnBinding mod_mul_binding = child_mul_bindings[0];
		mod_mul_binding.column_index = modified_node_logical_agg->groups.size();
		auto mult_group_by =
		    make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", pw.mul_type, mod_mul_binding);
		modified_node_logical_agg->groups.emplace_back(std::move(mult_group_by));

		auto mult_group_by_stats = make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(pw.mul_type));
		modified_node_logical_agg->group_stats.emplace_back(std::move(mult_group_by_stats));

		if (modified_node_logical_agg->grouping_sets.empty()) {
			modified_node_logical_agg->grouping_sets = {{0}};
		} else {
			idx_t gr = modified_node_logical_agg->grouping_sets[0].size();
			modified_node_logical_agg->grouping_sets[0].insert(gr);
		}

		mod_mul_binding.table_index = modified_node_logical_agg->group_index;
#ifdef DEBUG
		for (size_t i = 0; i < modified_node_logical_agg->GetColumnBindings().size(); i++) {
			printf("aggregate node CB after %zu %s\n", i,
			       modified_node_logical_agg->GetColumnBindings()[i].ToString().c_str());
		}
		printf("Modified plan (aggregate/group by):\n%s\nParameters:", pw.plan->ToString().c_str());
		// Output ParameterToString.
		for (const auto& i_param : pw.plan->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		printf("\n---end of modified plan (aggregate/group by)---\n");
#endif
		// Return plan, along with the modified multiplicity binding.
		return {std::move(pw.plan), mod_mul_binding};
	}
	/*
	 * END OF CASE.
	 */
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// FIXME: Review logic, and heavily reduce complexity.
		printf("\nIn logical projection case \n Add the multiplicity column to the second node...\n");
		printf("Modified plan (projection, start):\n%s\nParameters:", pw.plan->ToString().c_str());
		for (const auto& i_param : pw.plan->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		printf("\n---end of modified plan (projection)---\n");
		for (size_t i = 0; i < pw.plan->GetColumnBindings().size(); i++) {
			printf("Top node CB before %zu %s\n", i, pw.plan->GetColumnBindings()[i].ToString().c_str());
		}

		auto projection_node = dynamic_cast<LogicalProjection *>(pw.plan.get());
		printf("plan (of projection_node):\n%s\nParameters:", projection_node->ToString().c_str());
		for (const auto& i_param : projection_node->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		printf("\n---end of projection_node plan---\n");

		// the table_idx used to create ColumnBinding will be that of the top node's child
		// the column_idx used to create ColumnBinding for the multiplicity column will be stored using the context from the child
		// node
		auto e = make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", pw.mul_type, child_mul_bindings[0]);
		printf("Add mult column to exp\n");
		projection_node->expressions.emplace_back(std::move(e));

		printf("Modified plan (of projection_node):\n%s\nParameters:", projection_node->ToString().c_str());
		// Output ParameterToString.
		for (const auto& i_param : projection_node->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		printf("\n---end of modified plan (of projection_node)---\n");
		for (size_t i = 0; i < projection_node->GetColumnBindings().size(); i++) {
			printf("Top node CB %zu %s\n", i, projection_node->GetColumnBindings()[i].ToString().c_str());
		}
		break;
	}
	/*
	 * END OF CASE.
	 */
	case LogicalOperatorType::LOGICAL_FILTER: {
		// If the filter does nothing, ignore it completely.
		if (pw.plan->expressions.empty()) {
			return {std::move(pw.plan->children[0]), child_mul_bindings[0]};
		}
		// FIXME: If filter is NOT empty, the LOGICAL_FILTER should copy the bindings, projection map etc etc
		//  from its only child (whatever that child may be).
		break;
	}
	default:
		throw NotImplementedException("Operator type %s not supported", LogicalOperatorToString(pw.plan->type));
	}
	// Default: return the plan, along with the multiplicity column of the first (and hopefully only) child.
	return {std::move(pw.plan), child_mul_bindings[0]};
}

void IVMRewriteRule::IVMRewriteRuleFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan) {
	// first function call
	// the plan variable contains the plan for "SELECT * FROM DOIVM"
	if (plan->children.empty()) {
		return;
	}

	// check if plan contains table function `DoIVM`
	// The query to trigger IVM will be of the form `CREATE TABLE delta_view_name AS SELECT * from
	// DoIVM('view_name');` The plan's last child should be the DoIVM table function
	auto child = plan.get();
	while (!child->children.empty()) {
		child = child->children[0].get();
	}
	if (child->GetName().substr(0, 5) != "DOIVM") {
		return;
	}

#ifdef DEBUG
	printf("Activating the rewrite rule\n");
#endif

	auto child_get = dynamic_cast<LogicalGet *>(child);
	auto view = child_get->named_parameters["view_name"].ToString();
	auto view_catalog = child_get->named_parameters["view_catalog_name"].ToString();
	auto view_schema = child_get->named_parameters["view_schema_name"].ToString();

	// obtain view definition from catalog
	// generate the optimized logical plan
	Connection con(*input.context.db);

	con.BeginTransaction();
	// todo: maybe we want to disable more optimizers (internal_optimizer_types)
	con.Query("SET disabled_optimizers='compressed_materialization, statistics_propagation, expression_rewriter, filter_pushdown';");
	con.Commit();

	auto v = con.Query("select sql_string from _duckdb_ivm_views where view_name = '" + view + "';");
	if (v->HasError()) {
		throw InternalException("Error while querying view definition");
	}
	string view_query = v->GetValue(0, 0).ToString();

	Parser parser;
	Planner planner(input.context);

	parser.ParseQuery(view_query);
	auto statement = parser.statements[0].get();

	planner.CreatePlan(statement->Copy());
#ifdef DEBUG
	printf("Unoptimized plan: \n%s\n", planner.plan->ToString().c_str());
#endif
	Optimizer optimizer(*planner.binder, input.context);
	auto optimized_plan = optimizer.Optimize(std::move(planner.plan));
#ifdef DEBUG
	printf("Optimized plan: \n%s\n", optimized_plan->ToString().c_str());
#endif

	optional_ptr<CatalogEntry> table_catalog_entry = nullptr; // TODO: 2024-12-13 set but not used

	if (optimized_plan->children.empty()) {
		throw NotImplementedException("Plan contains single node, this is not supported");
	}

	/* +++ Old logic. (outdated as of December 2024) +++
	// recursively modify the optimized logical plan
	// IVM does the following steps:
	// 1) Replace the GET (scan) node with a new GET node that reads the delta table
	// 2) Add a filter with the timestamp (only taking the data updated after the last refresh)
	// 3) Add the multiplicity column all other nodes (aggregates etc.)
	// 4) Add the multiplicity column to the top projection node
	// 5) Add the insert node to the plan (to insert the query result in the delta table)
	// if there is no filter, we manually need to add one for the timestamp
	*/

	/* The IVM logic takes the following steps:
	 * 1. Replace the GET (scan) node with a new GET note, reading the delta table.
	 * 2. Add a timestamp filter expression to this new GET note
	 *     (i.e. only consider records updated after the last refresh)
	 * 3. Recursively add the multiplicity column upwards into the tree.
	 * 4. Replace any occurrences of joins with 3 joins (permutations with a delta on either/both sides)
	 * 5. Add an insert node to the top of the plan, such that the result can be inserted into the delta table.
	 */

#ifdef DEBUG
	std::cout << "Running ModifyPlan..." << '\n';
#endif
	auto root = optimized_plan.get();
	auto start_pw = PlanWrapper(input, optimized_plan, view, root);
	ModifiedPlan modified_plan = ModifyPlan(start_pw);
#ifdef DEBUG
	std::cout << "Running AddInsertNode..." << '\n';
#endif
	AddInsertNode(input.context, modified_plan.op, view, view_catalog, view_schema);
#ifdef DEBUG
	std::cout << "\nFINAL PLAN:\n" << modified_plan.op->ToString() << '\n';
#endif
	plan = std::move(modified_plan.op);
	return;
}
} // namespace duckdb.
