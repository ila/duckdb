#include "openivm_rewrite_rule.hpp"

// From DuckDB.
#include "../../postgres_scanner/include/postgres_scanner.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb.hpp"
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/optimizer/column_binding_replacer.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_comparison_join.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "openivm_index_regen.hpp"

// Std.
#include "../../third_party/zstd/include/zstd/common/debug.h"
#include "duckdb/planner/planner.hpp"
#include "duckdb/parser/parser.hpp"
#include <iostream>

namespace {
using duckdb::BoundColumnRefExpression;
using duckdb::ColumnBinding;
using duckdb::Expression;
using duckdb::JoinCondition;
using duckdb::LogicalComparisonJoin;
using duckdb::LogicalOperator;
using duckdb::LogicalType;
using duckdb::make_uniq;
using duckdb::unique_ptr;
using duckdb::vector;

/// Print the column bindings of a join, and each of its children, as well as the total count of bindings.
#ifdef DEBUG
void print_column_bindings(const unique_ptr<LogicalComparisonJoin> &join) {
	const auto join_bindings = join->GetColumnBindings();
	// lc/rc: left child and right child.
	const auto lc_bindings = join->children[0]->GetColumnBindings();
	const auto rc_bindings = join->children[1]->GetColumnBindings();
	const size_t join_cb_count = join_bindings.size();
	const size_t lc_cb_count = lc_bindings.size();
	const size_t rc_cb_count = rc_bindings.size();

	printf("Join CB count: %zu (left child: %zu, right child: %zu)\n", join_cb_count, lc_cb_count, rc_cb_count);
	for (size_t i = 0; i < lc_cb_count; i++) {
		printf("Left child CB after %zu %s\n", i, join_bindings[i].ToString().c_str());
	}
	for (size_t i = 0; i < rc_cb_count; i++) {
		printf("Right child CB after %zu %s\n", i, join_bindings[i].ToString().c_str());
	}
	for (size_t i = 0; i < join_cb_count; i++) {
		printf("Join CB after %zu %s\n", i, join_bindings[i].ToString().c_str());
	}
}
#endif

/// Create an empty join object from an existing comparison join, to work around the JoinRefType issue.
unique_ptr<LogicalComparisonJoin> create_empty_join(duckdb::ClientContext &context,
                                                    const unique_ptr<LogicalComparisonJoin> &current_join) {
	unique_ptr<LogicalComparisonJoin> copied_join =
	    duckdb::unique_ptr_cast<LogicalOperator, LogicalComparisonJoin>(current_join->Copy(context));
	// Note: Children are not cleared, just set to a nullptr. New children should be set, and not have emplace_back.
	copied_join->children[0].reset(nullptr);
	copied_join->children[1].reset(nullptr);
	copied_join->expressions.clear();
	// TODO: Check if needed (probably yes).
	copied_join->left_projection_map.clear();
	copied_join->right_projection_map.clear();
	return copied_join;
}

/// (for use in rebind_join_conditions only)
void rebind_bcr_if_needed(BoundColumnRefExpression &bcr, const std::unordered_map<idx_t, idx_t> &idx_map) {
	const idx_t table_index = bcr.binding.table_index;
	// Only replace if there is something to be mapped.
	if (idx_map.find(table_index) != idx_map.end()) {
		bcr.binding.table_index = idx_map.at(table_index);
	}
}

/// Rebind the column bindings in all expressions to a different table index (but same column index).
/// Does not modify the official vector, but returns a new vector instead.
vector<JoinCondition> rebind_join_conditions(const vector<JoinCondition> &original_conditions,
                                             const std::unordered_map<idx_t, idx_t> &idx_map) {
	vector<JoinCondition> return_vec;
	return_vec.reserve(original_conditions.size());
	for (const JoinCondition &cond : original_conditions) {
		unique_ptr<Expression> i_left = cond.left->Copy();
		unique_ptr<Expression> i_right = cond.right->Copy();
		if (cond.left->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
			// Get expression with replaced column type.
			// Code adapted from column_binding_replacer.cpp
			auto &left_bcr = i_left->Cast<BoundColumnRefExpression>();
			rebind_bcr_if_needed(left_bcr, idx_map);
		}
		if (cond.right->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
			auto &right_bcr = i_right->Cast<BoundColumnRefExpression>();
			rebind_bcr_if_needed(right_bcr, idx_map);
		}
		JoinCondition new_condition = JoinCondition();
		new_condition.left = std::move(i_left);
		new_condition.right = std::move(i_right);
		new_condition.comparison = cond.comparison;
		return_vec.emplace_back(std::move(new_condition));
	}
	return return_vec;
}

/*
/// Add the multiplicity column to a projection map if a projection map is defined.
void ensure_mul_binding(
    vector<idx_t>& projection_map,
    const vector<ColumnBinding> &child_bindings,
    const ColumnBinding& mul_binding) {
    if (!projection_map.empty()) {
        for (idx_t i = 0; i < child_bindings.size(); ++i) {
            if (child_bindings[i] == mul_binding) {
                projection_map.emplace_back(i);
                break;
            }
        }
    } // else: do nothing!
}
*/

/// Adjust the column order, such that the multiplicity column is at the end.
/// This vector of Expressions is meant to be used in conjunction with a LogicalProjection.
vector<unique_ptr<Expression>> project_multiplicity_to_end(const vector<ColumnBinding> &bindings,
                                                           const vector<LogicalType> &types,
                                                           const ColumnBinding &mul_binding) {
	assert(bindings.size() == types.size());
	const size_t col_count = bindings.size();
	// Note: slots are already made here, only need to be "populated".
	auto projection_col_refs = vector<unique_ptr<Expression>>(col_count);

	bool mul_is_seen = false;
	for (idx_t i = 0; i < col_count; ++i) {
		auto binding = bindings[i];
		unique_ptr<BoundColumnRefExpression> bound_col_ref = make_uniq<BoundColumnRefExpression>(types[i], binding);
		if (binding == mul_binding) {
			mul_is_seen = true;
			projection_col_refs[col_count - 1] = std::move(bound_col_ref);
		} else {
			const size_t insert_at = mul_is_seen ? i - 1 : i;
			projection_col_refs[insert_at] = std::move(bound_col_ref);
		}
	}
	return projection_col_refs;
}

/// Project out the multiplicity column of the dL side for dL JOIN dR, such that one multiplicity column remains.
vector<unique_ptr<Expression>> project_out_duplicate_mul_column(const vector<ColumnBinding> &bindings,
                                                                const vector<LogicalType> &types,
                                                                const ColumnBinding &redundant_mul_binding) {
	const size_t col_count = bindings.size();
	assert(col_count == types.size());

	// Create the vec and reserve the amount of elements (but don't create them yet).
	auto projection_col_refs = vector<unique_ptr<Expression>>();
	projection_col_refs.reserve(col_count - 1); // -1, since left mul removed.
	// Iterate over all columns. All but 1 should end up in the above vector.
	for (idx_t i = 0; i < col_count; ++i) {
		const auto &binding = bindings[i];
		if (binding != redundant_mul_binding) {
			projection_col_refs.emplace_back(make_uniq<BoundColumnRefExpression>(types[i], binding));
		}
	}
	return projection_col_refs;
}

/// Create a projection matching the column order of the join (only needed for Verify purposes).
vector<unique_ptr<Expression>> bindings_to_expressions(const vector<ColumnBinding> &bindings,
                                                       const vector<LogicalType> &types) {
	const size_t col_count = bindings.size();
	assert(col_count == types.size());

	// Create the vec and reserve the amount of elements (but don't create them yet).
	auto projection_col_refs = vector<unique_ptr<Expression>>();
	projection_col_refs.reserve(col_count);
	// Insert the columns. Mind the `-1`: the last element is omitted.
	for (idx_t i = 0; i < col_count; ++i) {
		projection_col_refs.emplace_back(make_uniq<BoundColumnRefExpression>(types[i], bindings[i]));
	}
	return projection_col_refs;
}

} // namespace

namespace duckdb {

void IVMRewriteRule::AddInsertNode(ClientContext &context, unique_ptr<LogicalOperator> &plan, string &view_name,
                                   string &view_catalog_name, string &view_schema_name) {
#ifdef DEBUG
	printf("\nAdd the insert node to the plan...\n");
	printf("Plan:\n%s\nParameters:", plan->ToString().c_str());
	// Get whatever ParameterToString yields.
	for (const auto &i_param : plan->ParamsToString()) {
		printf("%s", i_param.second.c_str());
	}
	printf("\n---end of insert node output---\n");
#endif

	auto delta_table_catalog_entry =
	    Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, view_catalog_name, view_schema_name, "delta_" + view_name,
	                      OnEntryNotFound::RETURN_NULL, QueryErrorContext());
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
	// Store the table indices of the original operator for usage in any column binding replacers much later.
	// Needed here, because the bindings of the operator may eventually change.
	const vector<ColumnBinding> original_bindings = pw.plan->GetColumnBindings();
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
	vector<ColumnBinding> child_mul_bindings;
	// Call each child of `plan` recursively (depth-first).
	for (auto &&child : pw.plan->children) {
		auto rec_pw = PlanWrapper(pw.input, child, pw.view, pw.root);
		ModifiedPlan child_plan = ModifyPlan(rec_pw);
		child = std::move(child_plan.op);
		child_mul_bindings.emplace_back(child_plan.mul_binding);
	}
	QueryErrorContext error_context = QueryErrorContext();

	// Rewrite operators depending on their type.
	switch (pw.plan->type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		/* Please note!
		 * These are the bindings AFTER ModifyPlan has made changes, meaning both sides have a multiplicity column.
		 * As a consequence, they need to be filtered out later.
		 * This should be doable with the provided multiplicity binding of both children.
		 */
		vector<ColumnBinding> modified_plan_bindings = pw.plan->GetColumnBindings();
		/* Ensure that the resulting types of each join is consistent.
		 * To help with that, create a copy of the `types` of pw.plan (which is a vec of LogicalType).
		 * This should be equivalent to the types of `L.*, R.*`
		 * The union, however, assumes the columns to be equivalent to `L.*, R.*, mul`.
		 * This means that mul must be added at some point before the union takes place.
		 */
		auto types = pw.plan->types;
		types.emplace_back(pw.mul_type); // Add bool type for multiplicity.
		// Cast plan into a LogicalComparisonJoin representing dL JOIN dR.
		printf("Modified plan (join, start):\n%s\nParameters:", pw.plan->ToString().c_str());
		for (const auto &i_param : pw.plan->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
#ifdef DEBUG
		printf("join detected. join child count: %zu\n", pw.plan->children.size());
		printf("plan left_child child count: %zu\n", left_child->children.size());
		printf("plan right_child child count: %zu\n", right_child->children.size());
#endif
		/* Suppose the query tree (below this join) is an L-side (left child) and an R side (right child).
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
		/* Steps:
		 * 1. Get delta children from modified plan, and store them in variables.
		 * 2. Manually create new joins, with copies of the necessary children
		 * -> 2a. Create join object with children
		 * -> 2b. Run rebinding procedure for them
		 * -> 2c. Create a projection with the correct binding (for union purposes).
		 * 3. Create the necessary Unions.
		 * 4. ColumnBindingReplacer for the stuff above the union (this logic can likely stay mostly the same).
		 */
		// FIXME: issues with creating join from scratch: (1) JoinRefType and (2) properly copying join conditions.
		// (1) Plan and delta children
		unique_ptr<LogicalComparisonJoin> plan_as_join =
		    unique_ptr_cast<LogicalOperator, LogicalComparisonJoin>(std::move(pw.plan));
		if (plan_as_join->join_type != JoinType::INNER) {
			throw Exception(ExceptionType::OPTIMIZER,
			                JoinTypeToString(plan_as_join->join_type) + " type not yet supported in OpenIVM");
		}
		// Define dL and dR references, for easier copies later.
		const unique_ptr<LogicalOperator> &delta_left = plan_as_join->children[0];
		const unique_ptr<LogicalOperator> &delta_right = plan_as_join->children[1];
		// Define the original column bindings for the multiplicity column of dL and dR.
		const ColumnBinding org_dl_mul = child_mul_bindings[0];
		const ColumnBinding org_dr_mul = child_mul_bindings[1];

		// (2) New joins. TODO: get rid of l/r/dl/dr variables (they are just there to make the code more readable).
		unique_ptr<LogicalProjection> dl_r_projected;
		// dLR
		{
			// dL: copy, then renumber.
			RenumberWrapper res = renumber_and_rebind_subtree(delta_left->Copy(context), pw.input.optimizer.binder);
			unique_ptr<LogicalOperator> dl = std::move(res.op);
			unique_ptr<LogicalOperator> r = right_child->Copy(context);

			unique_ptr<LogicalComparisonJoin> dl_r = create_empty_join(context, plan_as_join);
			// For the conditions: check what the original join has as conditions, and adapt any L columns to dL's.
			dl_r->conditions = rebind_join_conditions(plan_as_join->conditions, res.idx_map);
			// For the children: use dL and R.
			dl_r->children[0] = std::move(dl);
			dl_r->children[1] = std::move(r);
			dl_r->ResolveOperatorTypes();
#ifdef DEBUG
			printf("--- Column bindings of dL JOIN R after modifications (but before projection) ---\n");
			print_column_bindings(dl_r);
#endif
			// Create a projection.
			{
				const vector<ColumnBinding> join_bindings = dl_r->GetColumnBindings();
				const vector<LogicalType> join_types = dl_r->types;
				const ColumnBinding dl_mul_binding = {res.idx_map[org_dl_mul.table_index], org_dl_mul.column_index};
				vector<unique_ptr<Expression>> dl_r_projection_bindings =
				    project_multiplicity_to_end(join_bindings, join_types, dl_mul_binding);
				// Now, the vector with bindings should be complete. Let's put it in a Projection node!
				dl_r_projected = make_uniq<LogicalProjection>(pw.input.optimizer.binder.GenerateTableIndex(),
				                                              std::move(dl_r_projection_bindings));
			}
			dl_r_projected->children.emplace_back(std::move(dl_r));
			dl_r_projected->ResolveOperatorTypes();
			dl_r_projected->Verify(context);
#ifdef DEBUG
			auto projection_debug_bindings = dl_r_projected->GetColumnBindings();
			printf("dL-R projection CB count: %zu\n", projection_debug_bindings.size());
#endif
		}
		// LdR
		unique_ptr<LogicalProjection> l_dr_projected;
		{
			unique_ptr<LogicalOperator> l = left_child->Copy(context);
			// dR: copy, then renumber.
			RenumberWrapper res = renumber_and_rebind_subtree(delta_right->Copy(context), pw.input.optimizer.binder);
			unique_ptr<LogicalOperator> dr = std::move(res.op);

			unique_ptr<LogicalComparisonJoin> l_dr = create_empty_join(context, plan_as_join);
			// For the conditions: check what the original join has as conditions, and adapt any R columns to dR's.
			l_dr->conditions = rebind_join_conditions(plan_as_join->conditions, res.idx_map);
			// For the children: use L and dR.
			l_dr->children[0] = std::move(l);
			l_dr->children[1] = std::move(dr);
			l_dr->ResolveOperatorTypes();
#ifdef DEBUG
			printf("--- Column bindings of L JOIN dR after modifications (but before projection) ---\n");
			print_column_bindings(l_dr);
#endif
			// Make a projection. Although not necessary, it harmonises the shape of the query tree.
			{
				const vector<ColumnBinding> join_bindings = l_dr->GetColumnBindings();
				const vector<LogicalType> join_types = l_dr->types;
				vector<unique_ptr<Expression>> l_dr_projection_bindings =
				    bindings_to_expressions(join_bindings, join_types);
				l_dr_projected = make_uniq<LogicalProjection>(
					pw.input.optimizer.binder.GenerateTableIndex(), std::move(l_dr_projection_bindings)
                );
			}
			l_dr_projected->children.emplace_back(std::move(l_dr));
			l_dr_projected->ResolveOperatorTypes();
			l_dr_projected->Verify(context);
#ifdef DEBUG
			auto projection_debug_bindings = l_dr_projected->GetColumnBindings();
			printf("L-dR projection CB count: %zu\n", projection_debug_bindings.size());
#endif
		}
		// dLdR
		unique_ptr<LogicalProjection> dl_dr_projected;
		{
			// Same as above, but for both dL and dR
			RenumberWrapper dl_res = renumber_and_rebind_subtree(delta_left->Copy(context), pw.input.optimizer.binder);
			unique_ptr<LogicalOperator> dl = std::move(dl_res.op);
			RenumberWrapper dr_res = renumber_and_rebind_subtree(delta_right->Copy(context), pw.input.optimizer.binder);
			unique_ptr<LogicalOperator> dr = std::move(dr_res.op);
			// For the renumbering of the join conditions, we need the union of both operator's index maps.
			unique_ptr<LogicalComparisonJoin> dl_dr = create_empty_join(context, plan_as_join);
			{
				std::unordered_map<old_idx, new_idx> idx_map = dl_res.idx_map;
				for (const auto &pair : dr_res.idx_map) {
					idx_map.insert(pair);
				}
				dl_dr->conditions = rebind_join_conditions(plan_as_join->conditions, idx_map);
			}
			{
				// Add a special join condition (dL.mul = dR.mul) for correctness.
				ColumnBinding dl_mul_binding = {dl_res.idx_map[org_dl_mul.table_index], org_dl_mul.column_index};
				ColumnBinding dr_mul_binding = {dr_res.idx_map[org_dr_mul.table_index], org_dr_mul.column_index};
				JoinCondition mul_equal_condition;
				mul_equal_condition.left =
				    make_uniq<BoundColumnRefExpression>("left_mul", pw.mul_type, dl_mul_binding, 0);
				mul_equal_condition.right =
				    make_uniq<BoundColumnRefExpression>("right_mul", pw.mul_type, dr_mul_binding, 0);
				mul_equal_condition.comparison = ExpressionType::COMPARE_EQUAL;
				dl_dr->conditions.emplace_back(std::move(mul_equal_condition));
			}
			// For the children: use dL and dR.
			dl_dr->children[0] = std::move(dl);
			dl_dr->children[1] = std::move(dr);
			dl_dr->ResolveOperatorTypes();
#ifdef DEBUG
			printf("--- Column bindings of dL JOIN dR after modifications (but before projection) ---\n");
			print_column_bindings(dl_dr);
#endif
			// Create projection, such that the multiplicity column of dL goes out.
			// As a result, the projection should have one column binding less than the join.
			const vector<ColumnBinding> join_bindings = dl_dr->GetColumnBindings();
			{
				const vector<LogicalType> join_types = dl_dr->types;
				const ColumnBinding dl_mul_binding = {dl_res.idx_map[org_dl_mul.table_index], org_dl_mul.column_index};
				auto dl_dr_projection_bindings =
				    project_out_duplicate_mul_column(join_bindings, join_types, dl_mul_binding);
				dl_dr_projected = make_uniq<LogicalProjection>(
					pw.input.optimizer.binder.GenerateTableIndex(), std::move(dl_dr_projection_bindings)
                );
			}
			dl_dr_projected->children.emplace_back(std::move(dl_dr));
			dl_dr_projected->ResolveOperatorTypes();
			dl_dr_projected->Verify(context);
#ifdef DEBUG
			auto projection_debug_bindings = dl_dr_projected->GetColumnBindings();
			printf("dL-dR projection CB count: %zu\n", projection_debug_bindings.size());
			printf("Expected CB count: %zu - 1 = %zu (i.e. join bindings -1)\n", join_bindings.size(),
			       join_bindings.size() - 1);
#endif
		}
		// Now that all joins have the same columns, create a Union!
		auto copy_union = make_uniq<LogicalSetOperation>(pw.input.optimizer.binder.GenerateTableIndex(), types.size(),
		                                                 std::move(dl_r_projected), std::move(l_dr_projected),
		                                                 LogicalOperatorType::LOGICAL_UNION, true);
		copy_union->types = types;
		auto upper_u_table_index = pw.input.optimizer.binder.GenerateTableIndex();
		pw.plan = make_uniq<LogicalSetOperation>(upper_u_table_index, types.size(), std::move(copy_union),
		                                         std::move(dl_dr_projected), LogicalOperatorType::LOGICAL_UNION, true);
		pw.plan->types = types;
		printf("Modified plan (join, end):\n%s\nParameters:", pw.plan->ToString().c_str());
		for (const auto &i_param : pw.plan->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		// Rebind everything, because new joins have been implemented.
		ColumnBinding new_mul_binding;
		{
			// Two different bindings, don't confuse them!
			auto union_bindings = pw.plan->GetColumnBindings();
			if (union_bindings.size() - original_bindings.size() != 1) {
				throw InternalException(
				    "Union (with multiplicity column) should have exactly 1 more binding than original join!");
			}
			ColumnBindingReplacer replacer;
			vector<ReplacementBinding> &replacement_bindings = replacer.replacement_bindings;
			for (idx_t col_idx = 0; col_idx < original_bindings.size(); ++col_idx) {
				// Old binding should be 0.0 or 1.0 something. Taken from the initial state of pw.plan.
				const auto &old_binding = original_bindings[col_idx];
				const auto &new_binding = union_bindings[col_idx];
				replacement_bindings.emplace_back(old_binding, new_binding);
			}
#ifdef DEBUG
			// Print the replacement bindings.
			printf("\n--- Running a ColumnBindingReplacer after the Union ---\n");
			for (const auto &i_binding : replacement_bindings) {
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
			 * Once again, it is assumed that the multiplicity column is at the END of the union's bindings.
			 */
			new_mul_binding = union_bindings[union_bindings.size() - 1];
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
		/* New logic */
		unique_ptr<LogicalGet> delta_get_node;
		ColumnBinding new_mul_binding, timestamp_binding;
		string table_name;
		{
			// Get TableCatalogEntry using CatalogEntry. Check whether it is a nullptr first though.
			// Although cast later, cannot be put into a scope because it is a reference.
			optional_ptr<CatalogEntry> opt_catalog_entry;
			{
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
				opt_catalog_entry =
				    Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, delta_table_catalog, delta_table_schema,
				                      delta_table, OnEntryNotFound::RETURN_NULL, error_context);
				if (opt_catalog_entry == nullptr) {
					// if delta base table does not exist, return error. This also means there are no deltas to compute.
					throw Exception(ExceptionType::BINDER,
					                "Table " + delta_table + " does not exist, no deltas to compute!");
				}
			}
			TableCatalogEntry &table_entry = opt_catalog_entry->Cast<TableCatalogEntry>();
			table_name = table_entry.name;
			unique_ptr<FunctionData> bind_data;
			auto scan_function = table_entry.GetScanFunction(context, bind_data);

			// Define the return names and types.
			vector<LogicalType> return_types = {};
			vector<string> return_names = {};
			vector<ColumnIndex> column_ids = {};
			/* Add the original GET column_ids using the logic as explained below.*/
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
			// The column_ids, return_types, and return_names are now identical to the original GET.
			// For the delta GET, the multiplicity column and timestamp should be added,
			// as in theory they "can be returned by the table function".
			/* Multiplicity column */
			return_types.push_back(pw.mul_type);
			return_names.push_back("_duckdb_ivm_multiplicity");
			const auto mul_col_idx = ColumnIndex(column_ids.size());
			column_ids.push_back(mul_col_idx);
			new_mul_binding = ColumnBinding(old_get->table_index, mul_col_idx.GetPrimaryIndex());

			/* Timestamp column */
			return_types.push_back(LogicalType::TIMESTAMP);
			return_names.push_back("_duckdb_ivm_timestamp");
			const auto timestamp_idx = ColumnIndex(column_ids.size());
			column_ids.push_back(timestamp_idx);
			timestamp_binding = ColumnBinding(old_get->table_index, timestamp_idx.GetPrimaryIndex());

			// Finally, create the delta GET node.
			delta_get_node = make_uniq<LogicalGet>(
				old_get->table_index, // Will get renumbered later.
				scan_function,
				std::move(bind_data),
				std::move(return_types),
				std::move(return_names)
			);
			delta_get_node->SetColumnIds(std::move(column_ids));
		}
		delta_get_node->table_filters = std::move(old_get->table_filters); // this should be empty
		// Add a filter for the timestamp. The filtered column should not be passed through by the GET-node.
		Connection con(*context.db);
		con.SetAutoCommit(false);
		// we add a table filter
		auto timestamp_query = "select last_update from _duckdb_ivm_delta_tables where view_name = '" + pw.view +
		                       "' and table_name = '" + table_name + "';";
		auto r = con.Query(timestamp_query);
		if (r->HasError()) {
			throw InternalException("Error while querying last_update");
		}
		auto table_filter = make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, r->GetValue(0, 0));
		delta_get_node->table_filters.filters[timestamp_binding.column_index] = std::move(table_filter);

#ifdef DEBUG
		auto debug_bindings_1 = delta_get_node->GetColumnBindings();
#endif
		delta_get_node->projection_ids = old_get->projection_ids;
		// Add the multiplicity column to the projection IDs, if any projection IDs are defined.
		// The size is "past the end" of the old get projection IDs, and thus the projection ID of the mul column.
		if (!delta_get_node->projection_ids.empty()) {
			delta_get_node->projection_ids.emplace_back(old_get->projection_ids.size());
			/* 2025-04-15 comment out. Timestamp column should NOT be passed up the tree.
			 * It is not used beyond the scan's filter. Commended out here for future reference. */
			// delta_get_node->projection_ids.emplace_back(old_get->projection_ids.size() + 1); // timestamp column
		}
		// how to make this code resilient?
		delta_get_node->ResolveOperatorTypes();
#ifdef DEBUG
		auto debug_bindings_2 = delta_get_node->GetColumnBindings();
#endif
		delta_get_node->Verify(pw.input.context);
		return {std::move(delta_get_node), new_mul_binding};
	}
	/*
	 * END OF CASE.
	 */
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		LogicalAggregate& modified_node_logical_agg = static_cast<LogicalAggregate&>(*pw.plan);
#ifdef DEBUG
		for (size_t i = 0; i < modified_node_logical_agg.GetColumnBindings().size(); i++) {
			printf("aggregate node CB before %zu %s\n", i,
			       modified_node_logical_agg.GetColumnBindings()[i].ToString().c_str());
		}
		printf("Aggregate index: %zu Group index: %zu\n", modified_node_logical_agg.aggregate_index,
		       modified_node_logical_agg.group_index);
#endif

		auto mult_group_by =
		    make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", pw.mul_type, child_mul_bindings[0]);
		modified_node_logical_agg.groups.emplace_back(std::move(mult_group_by));

		auto mult_group_by_stats = make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(pw.mul_type));
		modified_node_logical_agg.group_stats.emplace_back(std::move(mult_group_by_stats));

		if (modified_node_logical_agg.grouping_sets.empty()) {
			modified_node_logical_agg.grouping_sets = {{0}};
		} else {
			idx_t gr = modified_node_logical_agg.grouping_sets[0].size();
			modified_node_logical_agg.grouping_sets[0].insert(gr);
		}



#ifdef DEBUG
		for (size_t i = 0; i < modified_node_logical_agg.GetColumnBindings().size(); i++) {
			printf("aggregate node CB after %zu %s\n", i,
			       modified_node_logical_agg.GetColumnBindings()[i].ToString().c_str());
		}
		printf("Modified plan (aggregate/group by):\n%s\nParameters:", pw.plan->ToString().c_str());
		// Output ParameterToString.
		for (const auto &i_param : pw.plan->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		printf("\n---end of modified plan (aggregate/group by)---\n");
#endif
		// Return plan, along with the modified multiplicity binding.
		pw.plan->ResolveOperatorTypes();
		pw.plan->Verify(pw.input.context);
		// Use group index (because mul is in GROUP BY).
		// Column index is `groups.size() - 1`, because it is at the end (and already inserted, so -1 for index).
		ColumnBinding mod_mul_binding = ColumnBinding(
			modified_node_logical_agg.group_index, modified_node_logical_agg.groups.size() - 1
		);
		return {std::move(pw.plan), mod_mul_binding};
	}
	/*
	 * END OF CASE.
	 */
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// FIXME: Review logic, and heavily reduce complexity.
		// Some printing
		printf("\nIn logical projection case \n Add the multiplicity column to the second node...\n");
		printf("Modified plan (projection, start):\n%s\nParameters:", pw.plan->ToString().c_str());
		for (const auto &i_param : pw.plan->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		printf("\n---end of modified plan (projection)---\n");
		const auto bindings = pw.plan->GetColumnBindings();
		for (size_t i = 0; i < bindings.size(); i++) {
			printf("Top node CB before %zu %s\n", i, bindings[i].ToString().c_str());
		}
		// Cast operator to logical projection, to then modify it.
		auto projection_node = unique_ptr_cast<LogicalOperator, LogicalProjection>(std::move(pw.plan));

		// Use the child's multiplicity column for the projection, by putting it at the end.
		auto mul_expression =
		    make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", pw.mul_type, child_mul_bindings[0]);
		printf("Add multiplicity column to expression\n");
		projection_node->expressions.emplace_back(std::move(mul_expression));

		printf("Modified plan (of projection_node):\n%s\nParameters:", projection_node->ToString().c_str());
		// Output ParameterToString.
		for (const auto &i_param : projection_node->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		printf("\n---end of modified plan (of projection_node)---\n");
		// Now that the multiplicity column is appended to the END of the projection,
		// the last column binding should be the one of the multiplicity column.
		// Therefore, take the last value from GetColumnBindings() to obtain the new multiplicity column.
		const auto new_bindings = projection_node->GetColumnBindings();
		for (size_t i = 0; i < new_bindings.size(); i++) {
			printf("Top node CB %zu %s\n", i, new_bindings[i].ToString().c_str());
		}
		auto new_mul_binding = new_bindings[new_bindings.size() - 1];
		projection_node->Verify(pw.input.context);
		return {std::move(projection_node), new_mul_binding};
	}
	/*
	 * END OF CASE.
	 */
	case LogicalOperatorType::LOGICAL_FILTER: {
		// If the filter does nothing, ignore it completely.
		if (pw.plan->expressions.empty()) {
			pw.plan->children[0]->Verify(pw.input.context);
			return {std::move(pw.plan->children[0]), child_mul_bindings[0]};
		}

		/* LogicalFilter gets its GetColumnBindings from its (only) child.
		 * For a non-empty filter, all that needs to be done is to ensure that the multiplicity column is passed
		 * through. If the filter has a projection map, the index of the multiplicity column should be appended to it.
		 * This is done by checking the CBs of the child, and finding the index corresponding to the multiplicity's CB.
		 * If the map is empty, all columns (including multiplicity) are taken from the child, so nothing needs to be
		 * done.
		 */
		unique_ptr<LogicalFilter> plan_as_filter = unique_ptr_cast<LogicalOperator, LogicalFilter>(std::move(pw.plan));
		plan_as_filter->ResolveOperatorTypes(); // Might not be needed, but does not hurt.
		if (!plan_as_filter->projection_map.empty()) {
#ifdef DEBUG
			auto filter_binds_before = plan_as_filter->GetColumnBindings();
			printf("LOGICAL_FILTER projection_map size before adding mul CB: %zu\n", filter_binds_before.size());
#endif
			auto child_binds = plan_as_filter->children[0]->GetColumnBindings();
			// Multiplicity column likely at the end, so use reverse for-loop.
			// Watch out for off-by-one errors; code is written this way because i >= 0 is always true for size_t.
			ColumnBinding mul_binding = child_mul_bindings[0]; // Only one child.
			idx_t mul_index = child_binds.size();              // Gets decremented at start of while-loop.
			bool mul_found = false;
			while (mul_found == false && mul_index > 0) {
				--mul_index;
				if (child_binds[mul_index] == mul_binding) {
					mul_found = true;
				};
			}
			if (!mul_found) {
				throw InternalException("Filter's child does not have multiplicity column!");
			}
			// Multiplicity column is present and found; add it to the projection map.
			plan_as_filter->projection_map.emplace_back(mul_index);
#ifdef DEBUG
			auto filter_binds_after = plan_as_filter->GetColumnBindings();
			printf("LOGICAL_FILTER projection_map size after adding mul CB: %zu\n", filter_binds_after.size());
#endif
		}
#ifdef DEBUG
		else {
			printf("LOGICAL_FILTER has no projection_map; do not modify anything.\n");
		}
#endif
		return {std::move(plan_as_filter), child_mul_bindings[0]};
	}
	default:
		throw NotImplementedException("Operator type %s not supported", LogicalOperatorToString(pw.plan->type));
	}
	// Default: return the plan, along with the multiplicity column of the first (and hopefully only) child.
	pw.plan->Verify(pw.input.context);
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
	// If column_lifetime is enabled, then the existence of duplicate table indices etc etc is verified.
	// However, it massively complicates the query tree which is not nice for further usage in OpenIVM.
	// Therefore, it should be run for verification purposes once in a while (otherwise, turn it off).
	const bool verify_column_lifetime = false;
	if (verify_column_lifetime) {
		// Really hacky fix to the table index reuse problem.
		for (size_t i = 0; i < 30; ++i) {
			input.optimizer.binder.GenerateTableIndex();
		}
		con.Query("SET disabled_optimizers='compressed_materialization, statistics_propagation, expression_rewriter, "
		          "filter_pushdown';");
	} else {
		con.Query("SET disabled_optimizers='compressed_materialization, column_lifetime, statistics_propagation, "
		          "expression_rewriter, filter_pushdown';");
	}
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

	// optional_ptr<CatalogEntry> table_catalog_entry = nullptr; // TODO: 2024-12-13 set but not used

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
